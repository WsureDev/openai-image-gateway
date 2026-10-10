import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, chmodSync } from 'node:fs';
import { dirname } from 'node:path';
import type { Task, ImageInput, ImageOutput, TaskEvent } from './protocol.js';
import type { Decision } from './types.js';
import { HISTORY_SAMPLES, recentDuration, type PerformanceEstimate } from './scheduler.js';
export class StateStore {
  readonly db: DatabaseSync;
  constructor(path: string) {
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    this.db = new DatabaseSync(path);
    chmodSync(path, 0o600);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, key TEXT UNIQUE, fingerprint TEXT NOT NULL, doc TEXT NOT NULL, request TEXT NOT NULL, result TEXT);
      CREATE TABLE IF NOT EXISTS decisions(job_id TEXT PRIMARY KEY, doc TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS events(worker_id TEXT NOT NULL, instance_id TEXT NOT NULL, sequence INTEGER NOT NULL, doc TEXT NOT NULL, PRIMARY KEY(worker_id,instance_id,sequence));
      CREATE TABLE IF NOT EXISTS worker_history(worker_id TEXT PRIMARY KEY, doc TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS artifacts(id TEXT PRIMARY KEY, content BLOB NOT NULL, mime TEXT NOT NULL, expires INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS scheduler_samples(
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT UNIQUE NOT NULL,
        worker_id TEXT NOT NULL, instance_id TEXT NOT NULL, model TEXT NOT NULL, operation TEXT NOT NULL,
        size TEXT NOT NULL, n INTEGER NOT NULL, duration REAL);
      CREATE INDEX IF NOT EXISTS scheduler_worker ON scheduler_samples(model,operation,worker_id,instance_id,sequence);
      CREATE INDEX IF NOT EXISTS scheduler_operation ON scheduler_samples(model,operation,sequence);
      CREATE TABLE IF NOT EXISTS adapter_configs(
        adapter_type TEXT NOT NULL,
        image_id TEXT NOT NULL,
        schema_json TEXT NOT NULL,
        config_json TEXT NOT NULL,
        PRIMARY KEY(adapter_type,image_id)
      );
      PRAGMA user_version=2;`);
    // Derive the initial observations from persisted jobs, excluding queue time and failures.
    // Already-recorded dispatches are never re-learned, including uncertain completions.
    this.db
      .exec(`INSERT OR IGNORE INTO scheduler_samples(job_id,worker_id,instance_id,model,operation,size,n,duration)
      SELECT id,json_extract(doc,'$.worker_id'),json_extract(doc,'$.instance_id'),
        json_extract(doc,'$.model'),json_extract(doc,'$.operation'),
        COALESCE(json_extract(request,'$.fields.size'),'auto'),COALESCE(json_extract(request,'$.fields.n'),1),
        CASE WHEN json_extract(doc,'$.state')='succeeded'
          AND json_extract(doc,'$.completed_at')>json_extract(doc,'$.started_at')
          THEN json_extract(doc,'$.completed_at')-json_extract(doc,'$.started_at') ELSE NULL END
      FROM jobs WHERE json_extract(doc,'$.started_at') IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM scheduler_samples WHERE job_id=jobs.id)
      ORDER BY jobs.rowid`);
  }
  close(): void {
    this.db.close();
  }
  create(task: Task, input: ImageInput): void {
    this.db
      .prepare('INSERT INTO jobs VALUES(?,?,?,?,?,NULL)')
      .run(
        task.id,
        task.key || null,
        task.fingerprint,
        JSON.stringify(task),
        JSON.stringify(input),
      );
  }
  save(task: Task, result?: ImageOutput): void {
    if (result)
      this.db
        .prepare('UPDATE jobs SET doc=?,result=? WHERE id=?')
        .run(JSON.stringify(task), JSON.stringify(result), task.id);
    else this.db.prepare('UPDATE jobs SET doc=? WHERE id=?').run(JSON.stringify(task), task.id);
  }
  task(id: string): Task | undefined {
    const row = this.db.prepare('SELECT doc FROM jobs WHERE id=?').get(id);
    return row ? JSON.parse(String(row.doc)) : undefined;
  }
  byKey(key: string): Task | undefined {
    const row = this.db.prepare('SELECT doc FROM jobs WHERE key=?').get(key);
    return row ? JSON.parse(String(row.doc)) : undefined;
  }
  input(id: string): ImageInput {
    return JSON.parse(
      String(this.db.prepare('SELECT request FROM jobs WHERE id=?').get(id)!.request),
    );
  }
  result(id: string): ImageOutput | undefined {
    const row = this.db.prepare('SELECT result FROM jobs WHERE id=?').get(id);
    return row?.result ? JSON.parse(String(row.result)) : undefined;
  }
  tasks(): Task[] {
    return this.db
      .prepare('SELECT doc FROM jobs ORDER BY rowid')
      .all()
      .map((r) => JSON.parse(String(r.doc)));
  }
  decision(value: Decision): void {
    this.db
      .prepare('INSERT OR REPLACE INTO decisions VALUES(?,?)')
      .run(value.job_id, JSON.stringify(value));
  }
  decisions(id?: string): Decision[] {
    return (
      id
        ? this.db.prepare('SELECT doc FROM decisions WHERE job_id=?').all(id)
        : this.db.prepare('SELECT doc FROM decisions ORDER BY rowid DESC LIMIT 100').all()
    ).map((r) => JSON.parse(String(r.doc)));
  }
  event(worker: string, instance: string, event: TaskEvent): boolean {
    return (
      Number(
        this.db
          .prepare('INSERT OR IGNORE INTO events VALUES(?,?,?,?)')
          .run(worker, instance, event.sequence, JSON.stringify(event)).changes,
      ) > 0
    );
  }
  events(worker?: string): unknown[] {
    return (
      worker
        ? this.db
            .prepare('SELECT doc FROM events WHERE worker_id=? ORDER BY rowid DESC LIMIT 100')
            .all(worker)
        : this.db.prepare('SELECT doc FROM events ORDER BY rowid DESC LIMIT 100').all()
    ).map((r) => JSON.parse(String(r.doc)));
  }
  worker(id: string, doc: unknown): void {
    this.db
      .prepare('INSERT OR REPLACE INTO worker_history VALUES(?,?)')
      .run(id, JSON.stringify(doc));
  }
  workers(): Record<string, unknown>[] {
    return this.db
      .prepare('SELECT doc FROM worker_history')
      .all()
      .map((r) => JSON.parse(String(r.doc)));
  }
  ensureAdapterConfig(
    adapterType: string,
    imageId: string,
    schema: unknown,
    defaults: unknown,
  ): void {
    this.db
      .prepare(
        'INSERT OR IGNORE INTO adapter_configs(adapter_type,image_id,schema_json,config_json) VALUES(?,?,?,?)',
      )
      .run(adapterType, imageId, JSON.stringify(schema), JSON.stringify(defaults));
  }
  adapterConfig(
    adapterType: string,
    imageId: string,
  ):
    | {
        adapter_type: string;
        image_id: string;
        schema: unknown;
        config: unknown;
      }
    | undefined {
    const row = this.db
      .prepare(
        'SELECT adapter_type,image_id,schema_json,config_json FROM adapter_configs WHERE adapter_type=? AND image_id=?',
      )
      .get(adapterType, imageId) as
      | { adapter_type: string; image_id: string; schema_json: string; config_json: string }
      | undefined;
    if (!row) return undefined;
    return {
      adapter_type: row.adapter_type,
      image_id: row.image_id,
      schema: JSON.parse(row.schema_json),
      config: JSON.parse(row.config_json),
    };
  }
  adapterConfigs(): Array<{
    adapter_type: string;
    image_id: string;
    schema: unknown;
    config: unknown;
  }> {
    return this.db
      .prepare(
        'SELECT adapter_type,image_id,schema_json,config_json FROM adapter_configs ORDER BY adapter_type,image_id',
      )
      .all()
      .map((row) => {
        const value = row as {
          adapter_type: string;
          image_id: string;
          schema_json: string;
          config_json: string;
        };
        return {
          adapter_type: value.adapter_type,
          image_id: value.image_id,
          schema: JSON.parse(value.schema_json),
          config: JSON.parse(value.config_json),
        };
      });
  }
  setAdapterConfig(adapterType: string, imageId: string, config: unknown): void {
    if (!config || typeof config !== 'object' || Array.isArray(config))
      throw new Error('adapter config must be a JSON object');
    const result = this.db
      .prepare('UPDATE adapter_configs SET config_json=? WHERE adapter_type=? AND image_id=?')
      .run(JSON.stringify(config), adapterType, imageId);
    if (Number(result.changes) !== 1) throw new Error('adapter config not registered');
  }
  dispatched(job: Task, input: ImageInput): void {
    this.db
      .prepare(
        `INSERT OR IGNORE INTO scheduler_samples(job_id,worker_id,instance_id,model,operation,size,n)
      VALUES(?,?,?,?,?,?,?)`,
      )
      .run(
        job.id,
        job.worker_id,
        job.instance_id,
        job.model,
        job.operation,
        String(input.fields.size ?? 'auto'),
        Number(input.fields.n ?? 1),
      );
  }
  recordDuration(job: Task, duration: number): void {
    if (Number.isFinite(duration) && duration > 0)
      this.db
        .prepare('UPDATE scheduler_samples SET duration=? WHERE job_id=?')
        .run(duration, job.id);
  }
  estimate(
    worker: string,
    instance: string,
    model: string,
    operation: string,
    input: ImageInput,
    fallback: number,
  ): PerformanceEstimate {
    const keys = [model, operation, worker, instance];
    const n = Number(input.fields.n ?? 1);
    const last = Number(
      this.db
        .prepare(
          `SELECT COALESCE(MAX(sequence),0) AS value FROM scheduler_samples
      WHERE model=? AND operation=? AND worker_id=? AND instance_id=?`,
        )
        .get(...keys)!.value,
    );
    const since = Number(
      this.db
        .prepare(
          `SELECT COUNT(*) AS value FROM scheduler_samples
      WHERE model=? AND operation=? AND sequence>?`,
        )
        .get(model, operation, last)!.value,
    );
    const exact = this.db
      .prepare(
        `SELECT duration FROM scheduler_samples
      WHERE model=? AND operation=? AND worker_id=? AND instance_id=? AND size=? AND n=? AND duration>0
      ORDER BY sequence DESC LIMIT ?`,
      )
      .all(...keys, String(input.fields.size ?? 'auto'), n, HISTORY_SAMPLES);
    const rows = exact.length
      ? exact
      : this.db
          .prepare(
            `SELECT duration/n*? AS duration FROM scheduler_samples
      WHERE model=? AND operation=? AND worker_id=? AND instance_id=? AND duration>0
      ORDER BY sequence DESC LIMIT ?`,
          )
          .all(n, ...keys, HISTORY_SAMPLES);
    return {
      estimated_ms: rows.length
        ? recentDuration(rows.reverse().map((row) => Number(row.duration)))
        : fallback,
      estimate_source: exact.length
        ? 'exact_history'
        : rows.length
          ? 'operation_history'
          : 'unmeasured',
      sample_count: rows.length,
      last_dispatch: last,
      dispatches_since_last: since,
    };
  }
  artifact(id: string, content: Buffer, mime: string, expires: number): void {
    this.db.prepare('INSERT INTO artifacts VALUES(?,?,?,?)').run(id, content, mime, expires);
  }
  getArtifact(id: string): { content: Buffer; mime: string } | undefined {
    const row = this.db
      .prepare('SELECT content,mime FROM artifacts WHERE id=? AND expires>?')
      .get(id, Date.now());
    return row
      ? { content: Buffer.from(row.content as Uint8Array), mime: String(row.mime) }
      : undefined;
  }
}
