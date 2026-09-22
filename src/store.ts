import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, chmodSync } from 'node:fs';
import { dirname } from 'node:path';
import type { Task, ImageInput, ImageOutput, TaskEvent } from './protocol.js';
import type { Decision } from './types.js';
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
      CREATE TABLE IF NOT EXISTS timings(profile TEXT PRIMARY KEY, duration REAL NOT NULL);
      PRAGMA user_version=1;`);
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
  duration(profile: string): number | undefined {
    const row = this.db.prepare('SELECT duration FROM timings WHERE profile=?').get(profile);
    return row ? Number(row.duration) : undefined;
  }
  sample(profile: string, duration: number): void {
    this.db
      .prepare(
        'INSERT INTO timings VALUES(?,?) ON CONFLICT(profile) DO UPDATE SET duration=0.8*duration+0.2*excluded.duration',
      )
      .run(profile, duration);
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
