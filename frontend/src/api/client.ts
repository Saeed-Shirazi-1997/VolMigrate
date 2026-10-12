import {
  Connection,
  VolumeInfo,
  NamespaceInfo,
  StorageClassInfo,
  PVCInfo,
  Migration,
  MigrationDetail,
  MigrationPlanResponse,
} from "../types";

const API_BASE = "/api";

export const api = {
  // Connections
  getConnections: async (): Promise<Connection[]> => {
    const res = await fetch(`${API_BASE}/connections`);
    return res.json();
  },
  createConnection: async (data: Partial<Connection>): Promise<Connection> => {
    const res = await fetch(`${API_BASE}/connections`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
    return res.json();
  },
  testConnection: async (id: string): Promise<{ success: boolean; version_info?: string; error?: string }> => {
    const res = await fetch(`${API_BASE}/connections/${id}/test`, { method: "POST" });
    return res.json();
  },
  deleteConnection: async (id: string): Promise<void> => {
    await fetch(`${API_BASE}/connections/${id}`, { method: "DELETE" });
  },

  // Discovery
  getVolumes: async (connId: string): Promise<VolumeInfo[]> => {
    const res = await fetch(`${API_BASE}/discovery/sources/${connId}/volumes`);
    return res.json();
  },
  getNamespaces: async (connId: string): Promise<NamespaceInfo[]> => {
    const res = await fetch(`${API_BASE}/discovery/destinations/${connId}/namespaces`);
    return res.json();
  },
  getStorageClasses: async (connId: string): Promise<StorageClassInfo[]> => {
    const res = await fetch(`${API_BASE}/discovery/destinations/${connId}/storage-classes`);
    return res.json();
  },
  getPVC: async (connId: string, ns: string, name: string): Promise<PVCInfo | null> => {
    const res = await fetch(`${API_BASE}/discovery/destinations/${connId}/namespaces/${ns}/pvcs/${name}`);
    if (res.status === 404) return null;
    return res.json();
  },

  // Migrations
  planMigration: async (planData: any): Promise<MigrationPlanResponse> => {
    const res = await fetch(`${API_BASE}/migrations/plan`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(planData),
    });
    return res.json();
  },
  getMigrations: async (): Promise<Migration[]> => {
    const res = await fetch(`${API_BASE}/migrations`);
    return res.json();
  },
  createMigration: async (data: any): Promise<Migration> => {
    const res = await fetch(`${API_BASE}/migrations`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
    return res.json();
  },
  getMigrationDetail: async (id: string): Promise<MigrationDetail> => {
    const res = await fetch(`${API_BASE}/migrations/${id}`);
    return res.json();
  },
  cancelMigration: async (id: string, reason?: string): Promise<void> => {
    await fetch(`${API_BASE}/migrations/${id}/cancel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason }),
    });
  },
  retryMigration: async (id: string): Promise<void> => {
    await fetch(`${API_BASE}/migrations/${id}/retry`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ force: false }),
    });
  },
};
