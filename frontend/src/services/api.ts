/**
 * API client for the Iceberg Metadata Visualizer backend.
 */

import axios, { AxiosInstance } from 'axios';
import type {
  CatalogCreate,
  CatalogInfo,
  CatalogTestResult,
  TableInfo,
  TableMetadata,
  SnapshotGraph,
  SnapshotInfo,
  SnapshotComparison,
  SnapshotDetails,
  ManifestListInfo,
  ManifestEntry,
  DataFileInfo,
  DataFileInspection,
  DataFileSample,
  PuffinFileInfo,
  TableStatistics,
  StorageAnalytics,
  OperationHistoryEntry,
} from '../types/iceberg';

// Prod build: empty baseURL → same-origin /api/... or ALB ingress.
const API_BASE_URL =
  (import.meta as any).env?.VITE_API_URL ||
  ((import.meta as any).env?.DEV ? 'http://localhost:8000' : '');

// Create axios instance
const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Catalog API
export const catalogApi = {
  create: async (data: CatalogCreate): Promise<CatalogInfo> => {
    const response = await apiClient.post<CatalogInfo>('/api/catalogs', data);
    return response.data;
  },

  list: async (): Promise<CatalogInfo[]> => {
    const response = await apiClient.get<CatalogInfo[]>('/api/catalogs');
    return response.data;
  },

  get: async (name: string): Promise<CatalogInfo> => {
    const response = await apiClient.get<CatalogInfo>(`/api/catalogs/${name}`);
    return response.data;
  },

  test: async (name: string): Promise<CatalogTestResult> => {
    const response = await apiClient.get<CatalogTestResult>(`/api/catalogs/${name}/test`);
    return response.data;
  },

  delete: async (name: string): Promise<void> => {
    await apiClient.delete(`/api/catalogs/${name}`);
  },

  createAsync: async (data: CatalogCreate): Promise<{ job_id: string; message: string }> => {
    const response = await apiClient.post<{ job_id: string; message: string }>('/api/catalogs/async', data);
    return response.data;
  },
};

// Job API
export interface JobStatus {
  id: string;
  type: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  progress: number;
  message: string;
  catalog?: string | null;
  payload?: Record<string, unknown> | null;
  result?: unknown;
  error?: string | null;
}

export const jobApi = {
  get: async (jobId: string): Promise<JobStatus> => {
    const response = await apiClient.get<JobStatus>(`/api/jobs/${jobId}`);
    return response.data;
  },

  list: async (limit = 100): Promise<JobStatus[]> => {
    const response = await apiClient.get<JobStatus[]>('/api/jobs', { params: { limit } });
    return response.data;
  },

  streamUrl: (jobId: string): string => `${API_BASE_URL}/api/jobs/${jobId}/stream`,

  delete: async (jobId: string): Promise<void> => {
    await apiClient.delete(`/api/jobs/${jobId}`);
  },
};

// Table API
export const tableApi = {
  listNamespaces: async (catalog: string): Promise<string[]> => {
    const response = await apiClient.get<string[]>('/api/tables/namespaces', {
      params: { catalog },
    });
    return response.data;
  },

  list: async (
    catalog: string,
    options?: {
      namespace?: string;
      lazy?: boolean;
      limit?: number;
      offset?: number;
    }
  ): Promise<TableInfo[]> => {
    const response = await apiClient.get<TableInfo[]>('/api/tables', {
      params: { catalog, ...options },
    });
    return response.data;
  },

  get: async (catalog: string, namespace: string, table: string): Promise<TableMetadata> => {
    const response = await apiClient.get<TableMetadata>(
      `/api/tables/${namespace}/${table}`,
      { params: { catalog } }
    );
    return response.data;
  },

  getRawMetadata: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<Record<string, unknown>> => {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/tables/${namespace}/${table}/metadata`,
      { params: { catalog } }
    );
    return response.data;
  },
};

// Snapshot API
export const snapshotApi = {
  getGraph: async (catalog: string, namespace: string, table: string): Promise<SnapshotGraph> => {
    const response = await apiClient.get<SnapshotGraph>(
      `/api/tables/${namespace}/${table}/snapshots`,
      { params: { catalog } }
    );
    return response.data;
  },

  get: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshotId: string
  ): Promise<SnapshotInfo> => {
    const response = await apiClient.get<SnapshotInfo>(
      `/api/tables/${namespace}/${table}/snapshots/${snapshotId}`,
      { params: { catalog } }
    );
    return response.data;
  },

  compare: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshot1: string,
    snapshot2: string
  ): Promise<SnapshotComparison> => {
    const response = await apiClient.post<SnapshotComparison>(
      `/api/tables/${namespace}/${table}/snapshots/compare`,
      null,
      { params: { catalog, snapshot1, snapshot2 } }
    );
    return response.data;
  },

  getDetails: async (
    catalog: string,
    namespace: string,
    table: string,
    entryLimit: number = 100
  ): Promise<SnapshotDetails[]> => {
    const response = await apiClient.get<SnapshotDetails[]>(
      `/api/tables/${namespace}/${table}/snapshots/details`,
      { params: { catalog, entry_limit: entryLimit } }
    );
    return response.data;
  },
};

// Manifest API
export const manifestApi = {
  getList: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshotId: string
  ): Promise<ManifestListInfo> => {
    const response = await apiClient.get<ManifestListInfo>(
      `/api/tables/${namespace}/${table}/snapshots/${snapshotId}/manifests`,
      { params: { catalog } }
    );
    return response.data;
  },

  getEntries: async (
    catalog: string,
    namespace: string,
    table: string,
    manifestPath: string,
    limit: number = 100
  ): Promise<ManifestEntry[]> => {
    const response = await apiClient.get<ManifestEntry[]>(
      `/api/tables/${namespace}/${table}/manifests`,
      { params: { catalog, path: manifestPath, limit } }
    );
    return response.data;
  },
};

// Data File API
export const dataFileApi = {
  list: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshotId: string,
    options?: {
      limit?: number;
      minSizeBytes?: number;
      maxSizeBytes?: number;
      fileFormat?: string;
    }
  ): Promise<DataFileInfo[]> => {
    const response = await apiClient.get<DataFileInfo[]>(
      `/api/tables/${namespace}/${table}/snapshots/${snapshotId}/files`,
      {
        params: {
          catalog,
          limit: options?.limit,
          min_size_bytes: options?.minSizeBytes,
          max_size_bytes: options?.maxSizeBytes,
          file_format: options?.fileFormat,
        },
      }
    );
    return response.data;
  },

  inspect: async (
    catalog: string,
    namespace: string,
    table: string,
    filePath: string
  ): Promise<DataFileInspection> => {
    const response = await apiClient.get<DataFileInspection>(
      `/api/tables/${namespace}/${table}/files/inspect`,
      { params: { catalog, path: filePath } }
    );
    return response.data;
  },

  sample: async (
    catalog: string,
    namespace: string,
    table: string,
    filePath: string,
    rows: number = 10
  ): Promise<DataFileSample> => {
    const response = await apiClient.get<DataFileSample>(
      `/api/tables/${namespace}/${table}/files/sample`,
      { params: { catalog, path: filePath, rows } }
    );
    return response.data;
  },
};

// Puffin/Statistics API
export const statisticsApi = {
  listFiles: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<PuffinFileInfo[]> => {
    const response = await apiClient.get<PuffinFileInfo[]>(
      `/api/tables/${namespace}/${table}/statistics`,
      { params: { catalog } }
    );
    return response.data;
  },

  get: async (
    catalog: string,
    namespace: string,
    table: string,
    snapshotId: string
  ): Promise<TableStatistics> => {
    const response = await apiClient.get<TableStatistics>(
      `/api/tables/${namespace}/${table}/statistics/${snapshotId}`,
      { params: { catalog } }
    );
    return response.data;
  },
};

// Health API
export type ScanMode = 'cached' | 'light' | 'full';

export interface TableHealthSummary {
  total_tables: number;
  healthy_tables: number;
  warning_tables: number;
  critical_tables: number;
  tables_needing_snapshot_expiration: number;
  tables_needing_compaction: number;
  tables_needing_manifest_rewrite: number;
  tables_with_delete_files: number;
  total_wasted_storage_gb: number;
  scan_mode?: string;
  cached_at?: string;
  cache_age_minutes?: number;
}

export interface HealthThresholds {
  snapshot_warning_threshold?: number;
  snapshot_critical_threshold?: number;
  snapshot_age_warning_days?: number;
  snapshot_age_critical_days?: number;
  small_file_size_mb?: number;
  small_file_warning_threshold?: number;
  small_file_critical_threshold?: number;
  delete_file_warning_threshold?: number;
  delete_file_critical_threshold?: number;
  small_manifest_file_count?: number;
  small_manifest_warning_threshold?: number;
}

export interface CacheInfo {
  catalog: string;
  cached_tables: number;
  cache_age_minutes: number | null;
  has_cache: boolean;
}

export interface CachedTableHealth {
  catalog: string;
  namespace: string;
  table_name: string;
  status: string;
  health_score: number;
  total_snapshots: number;
  total_data_files: number;
  total_delete_files: number;
  small_files_count: number;
  total_size_gb: number;
  avg_file_size_mb: number;
  oldest_snapshot_age_days: number | null;
  days_since_last_write: number | null;
  issues_count: number;
  warnings_count: number;
  scan_mode: string;
  scanned_at: string;
}

export interface TableHealth {
  catalog: string;
  namespace: string;
  table_name: string;
  status: 'healthy' | 'warning' | 'critical';
  health_score: number;
  metrics: {
    total_snapshots: number;
    oldest_snapshot_age_days: number | null;
    snapshots_last_7_days: number;
    snapshots_last_30_days: number;
    total_data_files: number;
    total_delete_files: number;
    small_files_count: number;
    avg_file_size_mb: number;
    total_size_gb: number;
    total_manifests: number;
    small_manifests_count: number;
    partition_count?: number | null;
    days_since_last_write?: number | null;
    total_records: number;
    total_position_deletes: number;
    total_equality_deletes: number;
    metadata_log_depth: number;
    schema_evolution_count: number;
    partition_spec_evolution_count: number;
    estimated_s3_cost_monthly: number;
  };
  recommendations: Array<{
    type: string;
    priority: string;
    reason: string;
    estimated_impact: string;
    command_example?: string | null;
  }>;
  issues_count: number;
  warnings_count: number;
  last_checked: string;
}

export interface ScanTriggerResponse {
  job_id: string;
  mode: string;
  message: string;
}

export interface ActiveHealthScan {
  job_id: string;
  mode: 'light' | 'full';
  status: 'pending' | 'running' | 'completed' | 'failed';
  progress: number;
  message: string;
  started_at: string;
}

export const healthApi = {
  getSummary: async (
    catalog: string,
    options?: {
      mode?: ScanMode;
      max_cache_age_minutes?: number;
      thresholds?: HealthThresholds;
    }
  ): Promise<TableHealthSummary> => {
    const response = await apiClient.get<TableHealthSummary>('/api/health/summary', {
      params: {
        catalog,
        mode: options?.mode,
        max_cache_age_minutes: options?.max_cache_age_minutes,
        ...options?.thresholds,
      },
    });
    return response.data;
  },

  getCacheInfo: async (catalog: string): Promise<CacheInfo> => {
    const response = await apiClient.get<CacheInfo>('/api/health/cache/info', {
      params: { catalog },
    });
    return response.data;
  },

  clearCache: async (catalog: string): Promise<{ deleted_tables: number }> => {
    const response = await apiClient.delete<{ deleted_tables: number }>('/api/health/cache', {
      params: { catalog },
    });
    return response.data;
  },

  getCachedTables: async (
    catalog: string,
    options?: {
      status_filter?: 'healthy' | 'warning' | 'critical';
      min_snapshots?: number;
      min_delete_files?: number;
      min_small_files?: number;
      limit?: number;
      offset?: number;
    }
  ): Promise<CachedTableHealth[]> => {
    const response = await apiClient.get<CachedTableHealth[]>('/api/health/tables/cached', {
      params: { catalog, ...options },
    });
    return response.data;
  },

  getCachedTable: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<CachedTableHealth | null> => {
    const response = await apiClient.get<CachedTableHealth | null>(
      `/api/health/tables/cached/${namespace}/${table}`,
      { params: { catalog } }
    );
    return response.data;
  },

  searchCachedTables: async (
    catalog: string,
    query: string,
    limit = 50
  ): Promise<Array<{ catalog: string; namespace: string; table_name: string }>> => {
    const response = await apiClient.get<Array<{ catalog: string; namespace: string; table_name: string }>>(
      '/api/health/tables/search',
      { params: { catalog, q: query, limit } }
    );
    return response.data;
  },

  getTableHealth: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<TableHealth> => {
    const response = await apiClient.get<TableHealth>(
      `/api/health/tables/${namespace}/${table}`,
      { params: { catalog } }
    );
    return response.data;
  },

  scanTable: async (
    catalog: string,
    namespace: string,
    table: string,
    mode: 'light' | 'full' = 'light'
  ): Promise<ScanTriggerResponse & { namespace: string; table: string }> => {
    const response = await apiClient.post<ScanTriggerResponse & { namespace: string; table: string }>(
      `/api/health/tables/${namespace}/${table}/scan`,
      null,
      { params: { catalog, mode } }
    );
    return response.data;
  },

  triggerScan: async (
    catalog: string,
    mode: 'light' | 'full' = 'light',
    thresholds?: HealthThresholds
  ): Promise<ScanTriggerResponse> => {
    const response = await apiClient.post<ScanTriggerResponse>('/api/health/scan/trigger', null, {
      params: {
        catalog,
        mode,
        ...thresholds,
      },
    });
    return response.data;
  },

  getActiveScan: async (catalog: string): Promise<ActiveHealthScan | null> => {
    const response = await apiClient.get<ActiveHealthScan | null>('/api/health/scan/active', {
      params: { catalog },
    });
    return response.data;
  },

  streamUrl: (
    catalog: string,
    mode: 'light' | 'full' = 'light',
    thresholds?: HealthThresholds
  ): string => {
    const params = new URLSearchParams({
      catalog,
      mode,
    });
    if (thresholds) {
      Object.entries(thresholds).forEach(([key, value]) => {
        if (value !== undefined) {
          params.append(key, String(value));
        }
      });
    }
    return `${API_BASE_URL}/api/health/summary/stream?${params.toString()}`;
  },
};

// Analytics API
export const analyticsApi = {
  getStorage: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<StorageAnalytics> => {
    const response = await apiClient.get<StorageAnalytics>(
      `/api/tables/${namespace}/${table}/analytics/storage`,
      { params: { catalog } }
    );
    return response.data;
  },

  getHistory: async (
    catalog: string,
    namespace: string,
    table: string
  ): Promise<OperationHistoryEntry[]> => {
    const response = await apiClient.get<OperationHistoryEntry[]>(
      `/api/tables/${namespace}/${table}/analytics/history`,
      { params: { catalog } }
    );
    return response.data;
  },
};

export default apiClient;
