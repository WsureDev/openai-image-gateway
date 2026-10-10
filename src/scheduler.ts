export interface PerformanceEstimate {
  estimated_ms: number;
  estimate_source: 'exact_history' | 'operation_history' | 'unmeasured';
  sample_count: number;
  last_dispatch: number;
  dispatches_since_last: number;
}
export interface RankedCandidate extends PerformanceEstimate {
  worker_id: string;
  eligible: boolean;
  reason?: string;
  in_flight: number;
  slots: number;
}
export const PROBE_INTERVAL = 20;
export const HISTORY_SAMPLES = 20;
export const HISTORY_ALPHA = 0.3;

/** Capacity first; history is only compared among equally occupied candidates. */
export function selectCandidate(candidates: RankedCandidate[]): {
  selected?: RankedCandidate;
  reason:
    | 'no_capacity'
    | 'new_worker_probe'
    | 'periodic_probe'
    | 'fastest_history'
    | 'unmeasured_available';
} {
  const eligible = candidates.filter((c) => c.eligible);
  if (!eligible.length) return { reason: 'no_capacity' };
  const minLoad = Math.min(...eligible.map((c) => c.in_flight / c.slots));
  const available = eligible.filter((c) => c.in_flight / c.slots === minLoad);
  const oldest = (a: RankedCandidate, b: RankedCandidate) => a.last_dispatch - b.last_dispatch;
  const fresh = available.filter((c) => c.last_dispatch === 0).sort(oldest);
  if (fresh.length) return { selected: fresh[0], reason: 'new_worker_probe' };
  const due = available.filter((c) => c.dispatches_since_last >= PROBE_INTERVAL).sort(oldest);
  if (due.length) return { selected: due[0], reason: 'periodic_probe' };
  const measured = available.filter((c) => c.sample_count > 0);
  if (measured.length)
    return {
      selected: measured.sort((a, b) => a.estimated_ms - b.estimated_ms || oldest(a, b))[0],
      reason: 'fastest_history',
    };
  return { selected: available.sort(oldest)[0], reason: 'unmeasured_available' };
}

/** Oldest-to-newest EWMA over a bounded recent history, independent of queue wait. */
export function recentDuration(values: number[]): number {
  return values.reduce(
    (average, value, index) =>
      index ? (1 - HISTORY_ALPHA) * average + HISTORY_ALPHA * value : value,
    0,
  );
}
