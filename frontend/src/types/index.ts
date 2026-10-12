export type ConnectionType = "docker" | "kubernetes";
export type ConnectionStatus = "active" | "error" | "untested";

export interface Connection {
  id: string;
  name: string;
  type: ConnectionType;
  endpoint?: string;
  status: ConnectionStatus;
  last_tested_at?: string;
  last_error?: string;
  created_at: string;
  updated_at: string;
}

export interface VolumeConsumer {
  container_id: string;
  container_name: string;
  status: string;
  is_running: boolean;
  mode: string;
}

export interface VolumeInfo {
  name: string;
  driver: string;
  mountpoint?: string;
  created_at?: string;
  labels: Record<string, string>;
  active_consumers_count: number;
  estimated_size_bytes?: number;
}

export interface NamespaceInfo {
  name: string;
  status: string;
}

export interface StorageClassInfo {
  name: string;
  provisioner: string;
  reclaim_policy?: string;
  volume_binding_mode?: string;
  is_default: boolean;
  allow_volume_expansion: boolean;
}

export interface PVCInfo {
  name: string;
  namespace: string;
  status: string;
  capacity_bytes?: number;
  storage_class_name?: string;
  access_modes: string[];
}

export interface PreflightCheckItem {
  name: string;
  passed: boolean;
  severity: "error" | "warning" | "info";
  message: string;
}

export interface PreflightCheckResult {
  passed: boolean;
  can_proceed: boolean;
  checks: PreflightCheckItem[];
}

export interface MigrationPlanResponse {
  plan_valid: boolean;
  source_volume_name: string;
  estimated_size_bytes?: number;
  active_consumers_count: number;
  dest_pvc_exists: boolean;
  dest_pvc_bound: boolean;
  preflight: PreflightCheckResult;
  warnings: string[];
}

export type MigrationStatus =
  | "draft"
  | "preflight"
  | "queued"
  | "preparing"
  | "transferring"
  | "validating"
  | "completed"
  | "failed"
  | "cancelled"
  | "needs_attention"
  | "cleanup_pending";

export interface Migration {
  id: string;
  name: string;
  source_connection_id: string;
  dest_connection_id: string;
  source_volume_name: string;
  dest_namespace: string;
  dest_pvc_name: string;
  storage_class_name?: string;
  capacity_bytes?: number;
  access_modes: string;
  status: MigrationStatus;
  current_step: string;
  transferred_bytes: number;
  total_bytes_estimated?: number;
  throughput_bytes_per_sec: number;
  validation_policy: string;
  overwrite_policy: string;
  error_message?: string;
  worker_id?: string;
  started_at?: string;
  completed_at?: string;
  created_at: string;
  updated_at: string;
}

export interface MigrationEvent {
  id: string;
  migration_id: string;
  step: string;
  level: "info" | "warning" | "error";
  message: string;
  payload?: string;
  created_at: string;
}

export interface MigrationDetail extends Migration {
  events: MigrationEvent[];
}
