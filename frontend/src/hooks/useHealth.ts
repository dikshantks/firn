import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { healthApi, HealthThresholds, ScanMode } from '../services/api';

// Query keys
export const healthKeys = {
  all: ['health'] as const,
  summary: (catalog: string, mode?: ScanMode) => ['health', 'summary', catalog, mode] as const,
  cacheInfo: (catalog: string) => ['health', 'cache', catalog] as const,
  activeScan: (catalog: string) => ['health', 'active-scan', catalog] as const,
  cachedTables: (catalog: string, filters?: Record<string, unknown>) => 
    ['health', 'tables', catalog, filters] as const,
  cachedTable: (catalog: string, namespace: string, table: string) =>
    ['health', 'cached-table', catalog, namespace, table] as const,
  table: (catalog: string, namespace: string, table: string) =>
    ['health', 'table', catalog, namespace, table] as const,
};

// Types for streaming events
export interface StreamingProgress {
  namespacesTotal: number;
  tablesTotal: number;
  mode: string;
}

export interface NamespaceResult {
  namespace: string;
  namespaceIndex: number;
  namespacesTotal: number;
  tablesScanned: number;
  tablesTotalScanned: number;
  tablesTotal: number;
  healthy: number;
  warning: number;
  critical: number;
  progressPercent: number;
}

export interface StreamingSummary {
  totalTables: number;
  healthyTables: number;
  warningTables: number;
  criticalTables: number;
  tablesNeedingSnapshotExpiration: number;
  tablesNeedingCompaction: number;
  tablesNeedingManifestRewrite: number;
  tablesWithDeleteFiles: number;
  totalWastedStorageGb: number;
  scanMode: string;
}

/**
 * Hook to get health summary with mode support.
 * 
 * Modes:
 * - cached: Return cached results instantly (default)
 * - light: Fresh scan using metadata only (fast)
 * - full: Fresh scan with S3 manifest reads (slow)
 */
export function useHealthSummary(
  catalog: string,
  options?: {
    mode?: ScanMode;
    max_cache_age_minutes?: number;
    thresholds?: HealthThresholds;
  }
) {
  return useQuery({
    queryKey: healthKeys.summary(catalog, options?.mode),
    queryFn: () => healthApi.getSummary(catalog, options),
    enabled: !!catalog,
    staleTime: options?.mode === 'cached' ? 5 * 60 * 1000 : 0,
  });
}

/**
 * Hook to get cache info for a catalog.
 */
export function useHealthCacheInfo(catalog: string) {
  return useQuery({
    queryKey: healthKeys.cacheInfo(catalog),
    queryFn: () => healthApi.getCacheInfo(catalog),
    enabled: !!catalog,
  });
}

export function useActiveHealthScan(catalog: string) {
  return useQuery({
    queryKey: healthKeys.activeScan(catalog),
    queryFn: () => healthApi.getActiveScan(catalog),
    enabled: !!catalog,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data || data.status === 'completed' || data.status === 'failed') {
        return false;
      }
      return 5000;
    },
  });
}

/**
 * Hook to get cached tables with filters.
 */
export function useCachedTables(
  catalog: string,
  options?: {
    status_filter?: 'healthy' | 'warning' | 'critical';
    min_snapshots?: number;
    min_delete_files?: number;
    min_small_files?: number;
    limit?: number;
    offset?: number;
  }
) {
  return useQuery({
    queryKey: healthKeys.cachedTables(catalog, options),
    queryFn: () => healthApi.getCachedTables(catalog, options),
    enabled: !!catalog,
  });
}

/**
 * Hook to check whether one table already has cached health data.
 */
export function useCachedTableHealth(catalog: string, namespace: string, table: string) {
  return useQuery({
    queryKey: healthKeys.cachedTable(catalog, namespace, table),
    queryFn: () => healthApi.getCachedTable(catalog, namespace, table),
    enabled: !!catalog && !!namespace && !!table,
    staleTime: 60 * 1000,
  });
}

/**
 * Hook to get detailed health for a specific table.
 */
export function useTableHealth(catalog: string, namespace: string, table: string, enabled = true) {
  return useQuery({
    queryKey: healthKeys.table(catalog, namespace, table),
    queryFn: () => healthApi.getTableHealth(catalog, namespace, table),
    enabled: enabled && !!catalog && !!namespace && !!table,
  });
}

/**
 * Hook to trigger a health scan for a specific table.
 */
export function useScanTableHealth() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      catalog,
      namespace,
      table,
      mode,
    }: {
      catalog: string;
      namespace: string;
      table: string;
      mode?: 'light' | 'full';
    }) => healthApi.scanTable(catalog, namespace, table, mode),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({
        queryKey: healthKeys.cachedTable(variables.catalog, variables.namespace, variables.table),
      });
      queryClient.invalidateQueries({
        queryKey: healthKeys.cachedTables(variables.catalog),
      });
      queryClient.invalidateQueries({
        queryKey: healthKeys.summary(variables.catalog),
      });
    },
  });
}

/**
 * Hook to trigger a health scan.
 */
export function useTriggerHealthScan() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: ({
      catalog,
      mode,
      thresholds,
    }: {
      catalog: string;
      mode?: 'light' | 'full';
      thresholds?: HealthThresholds;
    }) => healthApi.triggerScan(catalog, mode, thresholds),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: healthKeys.cacheInfo(variables.catalog) });
      queryClient.invalidateQueries({ queryKey: healthKeys.activeScan(variables.catalog) });
    },
  });
}

/**
 * Hook to clear health cache.
 */
export function useClearHealthCache() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: (catalog: string) => healthApi.clearCache(catalog),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: healthKeys.all });
    },
  });
}


