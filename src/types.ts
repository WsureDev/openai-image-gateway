export interface GatewayConfig {
  host: string;
  port: number;
  state_path: string;
  public_url: string;
  generation_token: string;
  enrollment_token: string;
  management_token: string;
  default_model?: string;
  heartbeat_ttl_ms: number;
  worker_request_timeout_ms: number;
  max_request_bytes: number;
  max_response_bytes: number;
  max_queued_jobs: number;
  queue_timeout_ms: number;
  poll_ms: number;
  allowed_worker_origins?: string[];
}
export interface Decision {
  job_id: string;
  selected_worker_id: string;
  created_at: number;
  candidates: Array<{
    worker_id: string;
    eligible: boolean;
    reason?: string;
    estimated_ms?: number;
  }>;
}
export type { Task, ImageInput, ImageOutput, Registration, Heartbeat, Model } from './protocol.js';
