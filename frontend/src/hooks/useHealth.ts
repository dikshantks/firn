import { useState, useEffect, useCallback, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { healthApi, HealthThresholds, ScanMode, TableHealthSummary } from '../services/api';

// Query keys
export const healthKeys = {
  all: ['health'] as const,
  summary: (catalog: string, mode?: ScanMode) => ['health', 'summary', catalog, mode] as const,
  cacheInfo: (catalog: string) => ['health', 'cache', catalog] as const,
  cachedTables: (catalog: string, filters?: Record<string, unknown>) => 
    ['health', 'tables', catalog, filters] as const,
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
    onSuccess: (_, catalog) => {
      queryClient.invalidateQueries({ queryKey: healthKeys.all });
    },
  });
}

/**
 * Hook for streaming health scan results namespace-by-namespace.
 * 
 * This provides real-time progress updates as each namespace is scanned,
 * allowing the UI to display partial results immediately.
 * 
 * @param catalog - Catalog name
 * @param options - Scan options including mode and thresholds
 * @returns Streaming state including progress, namespaces, summary, and controls
 */
export function useHealthSummaryStream(
  catalog: string,
  options?: {
    mode?: 'light' | 'full';
    thresholds?: HealthThresholds;
    autoStart?: boolean;
  }
) {
  const [isStreaming, setIsStreaming] = useState(false);
  const [progress, setProgress] = useState<StreamingProgress | null>(null);
  const [namespaces, setNamespaces] = useState<NamespaceResult[]>([]);
  const [summary, setSummary] = useState<StreamingSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [runningTotals, setRunningTotals] = useState({
    healthy: 0,
    warning: 0,
    critical: 0,
    tablesScanned: 0,
  });
  
  const eventSourceRef = useRef<EventSource | null>(null);
  const queryClient = useQueryClient();
  
  const reset = useCallback(() => {
    setProgress(null);
    setNamespaces([]);
    setSummary(null);
    setError(null);
    setRunningTotals({ healthy: 0, warning: 0, critical: 0, tablesScanned: 0 });
  }, []);
  
  const stop = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setIsStreaming(false);
  }, []);
  
  const start = useCallback(() => {
    if (!catalog) return;
    
    // Close existing connection
    stop();
    reset();
    
    const url = healthApi.streamUrl(catalog, options?.mode || 'light', options?.thresholds);
    const eventSource = new EventSource(url);
    eventSourceRef.current = eventSource;
    setIsStreaming(true);
    
    eventSource.addEventListener('progress', (event) => {
      try {
        const data = JSON.parse(event.data);
        setProgress({
          namespacesTotal: data.namespaces_total,
          tablesTotal: data.tables_total,
          mode: data.mode,
        });
      } catch (e) {
        console.error('Error parsing progress event:', e);
      }
    });
    
    eventSource.addEventListener('namespace_complete', (event) => {
      try {
        const data = JSON.parse(event.data);
        const result: NamespaceResult = {
          namespace: data.namespace,
          namespaceIndex: data.namespace_index,
          namespacesTotal: data.namespaces_total,
          tablesScanned: data.tables_scanned,
          tablesTotalScanned: data.tables_total_scanned,
          tablesTotal: data.tables_total,
          healthy: data.healthy,
          warning: data.warning,
          critical: data.critical,
          progressPercent: data.progress_percent,
        };
        
        setNamespaces((prev) => [...prev, result]);
        setRunningTotals((prev) => ({
          healthy: prev.healthy + data.healthy,
          warning: prev.warning + data.warning,
          critical: prev.critical + data.critical,
          tablesScanned: data.tables_total_scanned,
        }));
      } catch (e) {
        console.error('Error parsing namespace_complete event:', e);
      }
    });
    
    eventSource.addEventListener('scan_complete', (event) => {
      try {
        const data = JSON.parse(event.data);
        const summaryData = data.summary;
        setSummary({
          totalTables: summaryData.total_tables,
          healthyTables: summaryData.healthy_tables,
          warningTables: summaryData.warning_tables,
          criticalTables: summaryData.critical_tables,
          tablesNeedingSnapshotExpiration: summaryData.tables_needing_snapshot_expiration,
          tablesNeedingCompaction: summaryData.tables_needing_compaction,
          tablesNeedingManifestRewrite: summaryData.tables_needing_manifest_rewrite,
          tablesWithDeleteFiles: summaryData.tables_with_delete_files,
          totalWastedStorageGb: summaryData.total_wasted_storage_gb,
          scanMode: summaryData.scan_mode,
        });
        
        // Invalidate cache queries so they pick up new data
        queryClient.invalidateQueries({ queryKey: healthKeys.cacheInfo(catalog) });
        queryClient.invalidateQueries({ queryKey: healthKeys.summary(catalog) });
        
        eventSource.close();
        eventSourceRef.current = null;
        setIsStreaming(false);
      } catch (e) {
        console.error('Error parsing scan_complete event:', e);
      }
    });
    
    eventSource.addEventListener('error', (event) => {
      try {
        // Try to parse error data if available
        const messageEvent = event as MessageEvent;
        if (messageEvent.data) {
          const data = JSON.parse(messageEvent.data);
          setError(data.error || 'Unknown error occurred');
        }
      } catch {
        // EventSource error (connection issue)
        if (eventSource.readyState === EventSource.CLOSED) {
          setError('Connection closed unexpectedly');
        }
      }
      eventSource.close();
      eventSourceRef.current = null;
      setIsStreaming(false);
    });
    
    eventSource.onerror = () => {
      if (eventSource.readyState === EventSource.CLOSED && !summary) {
        setError('Connection lost');
        setIsStreaming(false);
      }
    };
  }, [catalog, options?.mode, options?.thresholds, queryClient, reset, stop, summary]);
  
  // Auto-start if enabled
  useEffect(() => {
    if (options?.autoStart && catalog) {
      start();
    }
    
    return () => {
      stop();
    };
  }, [options?.autoStart, catalog, start, stop]);
  
  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    };
  }, []);
  
  return {
    isStreaming,
    progress,
    namespaces,
    summary,
    runningTotals,
    error,
    start,
    stop,
    reset,
    isComplete: !!summary,
    progressPercent: progress?.tablesTotal 
      ? Math.round((runningTotals.tablesScanned / progress.tablesTotal) * 100)
      : 0,
  };
}
