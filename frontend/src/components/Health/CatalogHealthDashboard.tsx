import { useState, useEffect, useRef } from 'react';
import {
  Database,
  CheckCircle,
  XCircle,
  Loader2,
  ArrowLeft,
  Settings,
  ChevronDown,
  ChevronUp,
  RefreshCw,
  Clock,
  Zap,
  HardDrive,
  AlertTriangle,
  Trash2,
  FileStack,
  Files,
  Play,
  Square,
  Activity,
} from 'lucide-react';
import { 
  useHealthSummary, 
  useHealthCacheInfo, 
  useTriggerHealthScan,
  useHealthSummaryStream,
} from '../../hooks/useHealth';
import { useJobProgress } from '../../hooks/useJob';
import { JobProgress } from '../Common/JobProgress';
import type { HealthThresholds, ScanMode } from '../../services/api';

interface CatalogHealthDashboardProps {
  catalogName: string;
  onBack?: () => void;
  onViewTables?: (filter: { type: string; value?: number }) => void;
}

const DEFAULT_THRESHOLDS: HealthThresholds = {
  snapshot_warning_threshold: 50,
  snapshot_critical_threshold: 100,
  snapshot_age_warning_days: 30,
  snapshot_age_critical_days: 90,
  small_file_size_mb: 128,
  small_file_warning_threshold: 100,
  small_file_critical_threshold: 500,
  delete_file_warning_threshold: 10,
  delete_file_critical_threshold: 50,
  small_manifest_file_count: 10,
  small_manifest_warning_threshold: 20,
};

export function CatalogHealthDashboard({ catalogName, onBack, onViewTables }: CatalogHealthDashboardProps) {
  const [showConfig, setShowConfig] = useState(false);
  const [thresholds, setThresholds] = useState<HealthThresholds>(DEFAULT_THRESHOLDS);
  const [scanMode, setScanMode] = useState<ScanMode>('cached');
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [useStreaming, setUseStreaming] = useState(false);
  const [streamingMode, setStreamingMode] = useState<'light' | 'full'>('light');
  const autoScanStartedRef = useRef(false);
  
  const { data: summary, isLoading, error, refetch } = useHealthSummary(catalogName, {
    mode: scanMode,
    thresholds,
  });
  
  const { data: cacheInfo, isLoading: isCacheLoading, refetch: refetchCacheInfo } =
    useHealthCacheInfo(catalogName);
  const triggerScan = useTriggerHealthScan();
  useJobProgress(activeJobId);
  
  // Streaming hook
  const streaming = useHealthSummaryStream(catalogName, {
    mode: streamingMode,
    thresholds,
  });

  const handleStartStreaming = (mode: 'light' | 'full') => {
    setStreamingMode(mode);
    setUseStreaming(true);
    streaming.reset();
    setTimeout(() => streaming.start(), 0);
  };

  // Auto-start a light scan when no cached health data exists.
  useEffect(() => {
    if (
      autoScanStartedRef.current ||
      isCacheLoading ||
      streaming.isStreaming ||
      useStreaming
    ) {
      return;
    }

    const noCachedData = cacheInfo?.has_cache === false;
    const summaryUnavailable = Boolean(error) && !summary;

    if (noCachedData || summaryUnavailable) {
      autoScanStartedRef.current = true;
      handleStartStreaming('light');
    }
  }, [
    isCacheLoading,
    cacheInfo?.has_cache,
    error,
    summary,
    streaming.isStreaming,
    useStreaming,
  ]);

  const handleStopStreaming = () => {
    streaming.stop();
    setUseStreaming(false);
  };

  const handleStreamingComplete = () => {
    setUseStreaming(false);
    refetch();
    refetchCacheInfo();
  };

  // When streaming completes, refresh data
  useEffect(() => {
    if (streaming.isComplete && useStreaming) {
      handleStreamingComplete();
    }
  }, [streaming.isComplete, useStreaming]);

  // Background job-based scan (fallback, kept for future use)
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const _handleTriggerScan = async (mode: 'light' | 'full') => {
    try {
      const result = await triggerScan.mutateAsync({
        catalog: catalogName,
        mode,
        thresholds,
      });
      setActiveJobId(result.job_id);
    } catch (err) {
      console.error('Failed to trigger scan:', err);
    }
  };

  const handleJobComplete = () => {
    setActiveJobId(null);
    refetch();
    refetchCacheInfo();
  };

  const handleThresholdChange = (key: keyof HealthThresholds, value: string) => {
    const numValue = value === '' ? undefined : parseInt(value, 10);
    setThresholds((prev: HealthThresholds) => ({
      ...prev,
      [key]: numValue,
    }));
  };

  const resetToDefaults = () => {
    setThresholds(DEFAULT_THRESHOLDS);
  };

  // Show streaming UI when actively streaming
  if (streaming.isStreaming || (useStreaming && !streaming.isComplete)) {
    return (
      <div className="p-6 max-w-5xl mx-auto">
        {onBack && (
          <button
            type="button"
            onClick={onBack}
            className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 mb-4"
          >
            <ArrowLeft className="w-4 h-4" />
            Back
          </button>
        )}

        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-3">
            <Database className="w-10 h-10 text-iceberg" />
            <div>
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
                Scanning Health...
              </h2>
              <p className="text-sm text-gray-500">{catalogName}</p>
            </div>
          </div>
          <button
            onClick={handleStopStreaming}
            className="flex items-center gap-2 px-3 py-2 text-sm bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400 rounded-lg hover:bg-red-200 dark:hover:bg-red-900/50"
          >
            <Square className="w-4 h-4" />
            Stop Scan
          </button>
        </div>

        {/* Streaming Progress */}
        <div className="mb-6 bg-gray-50 dark:bg-gray-800/50 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <Activity className="w-5 h-5 text-iceberg animate-pulse" />
              <span className="font-medium text-gray-900 dark:text-white">
                {streamingMode === 'light' ? 'Light Scan' : 'Full Scan'} in Progress
              </span>
            </div>
            <span className="text-sm font-semibold text-iceberg">
              {streaming.progressPercent}%
            </span>
          </div>
          
          {/* Progress Bar */}
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-3 mb-3 overflow-hidden">
            <div
              className="h-3 rounded-full bg-iceberg transition-all duration-300"
              style={{ width: `${streaming.progressPercent}%` }}
            />
          </div>
          
          {/* Progress Details */}
          {streaming.progress && (
            <div className="flex flex-wrap gap-4 text-sm text-gray-600 dark:text-gray-400">
              <span>
                Namespaces: {streaming.namespaces.length} / {streaming.progress.namespacesTotal.toLocaleString()}
              </span>
              <span>
                Tables: {streaming.runningTotals.tablesScanned.toLocaleString()} / {streaming.progress.tablesTotal.toLocaleString()}
              </span>
            </div>
          )}
          
          {/* Current Namespace */}
          {streaming.namespaces.length > 0 && (
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-2">
              Last completed: <span className="font-medium">{streaming.namespaces[streaming.namespaces.length - 1].namespace}</span>
            </p>
          )}
        </div>

        {/* Live Running Totals */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg p-4 border border-gray-200 dark:border-gray-700 shadow-sm">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm text-gray-500 dark:text-gray-400">Scanned</span>
              <Database className="w-5 h-5 text-gray-400" />
            </div>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {streaming.runningTotals.tablesScanned.toLocaleString()}
            </p>
          </div>

          <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4 border border-green-200 dark:border-green-800">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm text-green-600 dark:text-green-400">Healthy</span>
              <CheckCircle className="w-5 h-5 text-green-500" />
            </div>
            <p className="text-2xl font-bold text-green-700 dark:text-green-300">
              {streaming.runningTotals.healthy.toLocaleString()}
            </p>
          </div>

          <div className="bg-yellow-50 dark:bg-yellow-900/20 rounded-lg p-4 border border-yellow-200 dark:border-yellow-800">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm text-yellow-600 dark:text-yellow-400">Warning</span>
              <AlertTriangle className="w-5 h-5 text-yellow-500" />
            </div>
            <p className="text-2xl font-bold text-yellow-700 dark:text-yellow-300">
              {streaming.runningTotals.warning.toLocaleString()}
            </p>
          </div>

          <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-4 border border-red-200 dark:border-red-800">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm text-red-600 dark:text-red-400">Critical</span>
              <XCircle className="w-5 h-5 text-red-500" />
            </div>
            <p className="text-2xl font-bold text-red-700 dark:text-red-300">
              {streaming.runningTotals.critical.toLocaleString()}
            </p>
          </div>
        </div>

        {/* Recent Namespaces */}
        {streaming.namespaces.length > 0 && (
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 shadow-sm overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
              <h3 className="font-semibold text-gray-900 dark:text-white">
                Recently Scanned Namespaces
              </h3>
            </div>
            <div className="max-h-64 overflow-y-auto">
              {streaming.namespaces.slice(-10).reverse().map((ns) => (
                <div
                  key={ns.namespace}
                  className="flex items-center justify-between px-4 py-2 border-b border-gray-100 dark:border-gray-700 last:border-b-0"
                >
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-gray-900 dark:text-white">
                      {ns.namespace}
                    </span>
                    <span className="text-xs text-gray-500">
                      ({ns.tablesScanned} tables)
                    </span>
                  </div>
                  <div className="flex items-center gap-3 text-xs">
                    <span className="text-green-600">{ns.healthy} healthy</span>
                    <span className="text-yellow-600">{ns.warning} warning</span>
                    <span className="text-red-600">{ns.critical} critical</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {streaming.error && (
          <div className="mt-4 p-4 bg-red-50 dark:bg-red-900/20 rounded-lg border border-red-200 dark:border-red-800">
            <p className="text-sm text-red-700 dark:text-red-400">
              Error: {streaming.error}
            </p>
          </div>
        )}
      </div>
    );
  }

  // Show loading while cache info is fetched or auto-scan is starting.
  if ((isCacheLoading || (cacheInfo?.has_cache === false && !streaming.isStreaming && !useStreaming)) && !summary) {
    return (
      <div className="flex flex-col items-center justify-center h-full p-8">
        <Loader2 className="w-12 h-12 animate-spin text-iceberg mb-4" />
        <p className="text-gray-500 dark:text-gray-400">Preparing health scan...</p>
      </div>
    );
  }

  // Show error state with manual scan options if auto-scan did not start.
  if (error && !summary && !streaming.isStreaming && !useStreaming) {
    const isNoCache = cacheInfo?.has_cache === false;
    
    return (
      <div className="p-6 max-w-5xl mx-auto">
        {onBack && (
          <button
            type="button"
            onClick={onBack}
            className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 mb-4"
          >
            <ArrowLeft className="w-4 h-4" />
            Back
          </button>
        )}

        <div className="flex flex-col items-center justify-center py-12">
          <Database className="w-16 h-16 text-gray-300 dark:text-gray-600 mb-4" />
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
            {isNoCache ? 'No Health Data Available' : 'Failed to Load Health Data'}
          </h3>
          <p className="text-sm text-gray-500 dark:text-gray-400 text-center max-w-md mb-6">
            {isNoCache 
              ? 'Start a health scan to analyze your catalog. For large catalogs, streaming mode shows results in real-time as each namespace is scanned.'
              : 'There was an error loading the health data. Try running a new scan.'}
          </p>
          
          <div className="flex flex-col sm:flex-row gap-3">
            <button
              onClick={() => handleStartStreaming('light')}
              className="flex items-center justify-center gap-2 px-4 py-2 bg-iceberg text-white rounded-lg hover:bg-iceberg/90 transition-colors"
            >
              <Play className="w-4 h-4" />
              Start Light Scan (Streaming)
            </button>
            <button
              onClick={() => handleStartStreaming('full')}
              className="flex items-center justify-center gap-2 px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 transition-colors"
            >
              <HardDrive className="w-4 h-4" />
              Start Full Scan (Streaming)
            </button>
          </div>
          
          <p className="text-xs text-gray-400 dark:text-gray-500 mt-4">
            Light scan uses metadata only (fast). Full scan reads S3 manifests for small file detection (slow).
          </p>
        </div>
      </div>
    );
  }

  // Show loading only for non-cached modes
  if (isLoading && scanMode !== 'cached') {
    return (
      <div className="flex flex-col items-center justify-center h-full p-8">
        <Loader2 className="w-12 h-12 animate-spin text-iceberg mb-4" />
        <p className="text-gray-500 dark:text-gray-400">
          {scanMode === 'light' ? 'Running light scan (metadata only)...' : 'Running full scan...'}
        </p>
        <p className="text-sm text-gray-400 dark:text-gray-500 mt-2">
          This may take a few minutes for large catalogs
        </p>
      </div>
    );
  }

  const okCount = summary?.healthy_tables ?? 0;
  const totalTables = summary?.total_tables ?? 0;
  const healthyPercent = totalTables > 0 ? Math.round((okCount / totalTables) * 100) : 0;

  return (
    <div className="p-6 max-w-5xl mx-auto">
      {onBack && (
        <button
          type="button"
          onClick={onBack}
          className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 mb-4"
        >
          <ArrowLeft className="w-4 h-4" />
          Back
        </button>
      )}

      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Database className="w-10 h-10 text-iceberg" />
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Data Lake Health Dashboard
            </h2>
            <p className="text-sm text-gray-500">{catalogName}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setShowConfig(!showConfig)}
            className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
          >
            <Settings className="w-4 h-4" />
            {showConfig ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          </button>
        </div>
      </div>

      {/* Cache Status & Scan Controls */}
      <div className="mb-6 bg-gray-50 dark:bg-gray-800/50 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2 text-sm">
              <Clock className="w-4 h-4 text-gray-400" />
              {cacheInfo?.has_cache ? (
                <span className="text-gray-600 dark:text-gray-400">
                  Last scan: <span className="font-medium">{cacheInfo.cache_age_minutes} min ago</span>
                  {summary?.scan_mode && (
                    <span className="ml-1 text-xs px-2 py-0.5 bg-gray-200 dark:bg-gray-700 rounded">
                      {summary.scan_mode}
                    </span>
                  )}
                </span>
              ) : (
                <span className="text-yellow-600 dark:text-yellow-400">No cached data</span>
              )}
            </div>
            {cacheInfo?.cached_tables !== undefined && cacheInfo.cached_tables > 0 && (
              <span className="text-sm text-gray-500">
                {cacheInfo.cached_tables.toLocaleString()} tables cached
              </span>
            )}
          </div>
          
          <div className="flex items-center gap-2">
            <button
              onClick={() => {
                setScanMode('cached');
                refetch();
              }}
              disabled={isLoading}
              className="px-3 py-1.5 text-sm text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-lg transition-colors flex items-center gap-1.5"
            >
              <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
            <button
              onClick={() => handleStartStreaming('light')}
              disabled={streaming.isStreaming || !!activeJobId}
              className="px-3 py-1.5 text-sm bg-iceberg/10 text-iceberg hover:bg-iceberg/20 rounded-lg transition-colors flex items-center gap-1.5 disabled:opacity-50"
            >
              <Zap className="w-4 h-4" />
              Light Scan
            </button>
            <button
              onClick={() => handleStartStreaming('full')}
              disabled={streaming.isStreaming || !!activeJobId}
              className="px-3 py-1.5 text-sm bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400 hover:bg-orange-200 dark:hover:bg-orange-900/50 rounded-lg transition-colors flex items-center gap-1.5 disabled:opacity-50"
            >
              <HardDrive className="w-4 h-4" />
              Full Scan
            </button>
          </div>
        </div>

        {/* Background Job Progress (fallback) */}
        {activeJobId && (
          <div className="mt-4">
            <JobProgress
              jobId={activeJobId}
              onComplete={handleJobComplete}
              onError={() => setActiveJobId(null)}
            />
          </div>
        )}
      </div>

      {/* Configuration Panel */}
      {showConfig && (
        <div className="mb-6 bg-gray-50 dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Health Threshold Configuration
            </h3>
            <button
              type="button"
              onClick={resetToDefaults}
              className="text-sm text-iceberg hover:text-iceberg/80"
            >
              Reset to Defaults
            </button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Snapshot Thresholds */}
            <div className="space-y-3">
              <h4 className="font-medium text-gray-900 dark:text-white text-sm border-b border-gray-200 dark:border-gray-700 pb-2">
                Snapshots
              </h4>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Warning</label>
                <input
                  type="number"
                  value={thresholds.snapshot_warning_threshold || ''}
                  onChange={(e) => handleThresholdChange('snapshot_warning_threshold', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded focus:ring-1 focus:ring-iceberg dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Critical</label>
                <input
                  type="number"
                  value={thresholds.snapshot_critical_threshold || ''}
                  onChange={(e) => handleThresholdChange('snapshot_critical_threshold', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded focus:ring-1 focus:ring-iceberg dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
            </div>

            {/* Age Thresholds */}
            <div className="space-y-3">
              <h4 className="font-medium text-gray-900 dark:text-white text-sm border-b border-gray-200 dark:border-gray-700 pb-2">
                Snapshot Age (days)
              </h4>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Warning</label>
                <input
                  type="number"
                  value={thresholds.snapshot_age_warning_days || ''}
                  onChange={(e) => handleThresholdChange('snapshot_age_warning_days', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded focus:ring-1 focus:ring-iceberg dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Critical</label>
                <input
                  type="number"
                  value={thresholds.snapshot_age_critical_days || ''}
                  onChange={(e) => handleThresholdChange('snapshot_age_critical_days', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded focus:ring-1 focus:ring-iceberg dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
            </div>

            {/* Small Files */}
            <div className="space-y-3">
              <h4 className="font-medium text-gray-900 dark:text-white text-sm border-b border-gray-200 dark:border-gray-700 pb-2">
                Small Files
              </h4>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Size (MB)</label>
                <input
                  type="number"
                  value={thresholds.small_file_size_mb || ''}
                  onChange={(e) => handleThresholdChange('small_file_size_mb', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded focus:ring-1 focus:ring-iceberg dark:bg-gray-700 dark:text-white"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Warning Count</label>
                <input
                  type="number"
                  value={thresholds.small_file_warning_threshold || ''}
                  onChange={(e) => handleThresholdChange('small_file_warning_threshold', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded focus:ring-1 focus:ring-iceberg dark:bg-gray-700 dark:text-white"
                  min="0"
                />
              </div>
            </div>

            {/* Delete Files */}
            <div className="space-y-3">
              <h4 className="font-medium text-gray-900 dark:text-white text-sm border-b border-gray-200 dark:border-gray-700 pb-2">
                Delete Files
              </h4>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Warning</label>
                <input
                  type="number"
                  value={thresholds.delete_file_warning_threshold || ''}
                  onChange={(e) => handleThresholdChange('delete_file_warning_threshold', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded focus:ring-1 focus:ring-iceberg dark:bg-gray-700 dark:text-white"
                  min="0"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Critical</label>
                <input
                  type="number"
                  value={thresholds.delete_file_critical_threshold || ''}
                  onChange={(e) => handleThresholdChange('delete_file_critical_threshold', e.target.value)}
                  className="w-full px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded focus:ring-1 focus:ring-iceberg dark:bg-gray-700 dark:text-white"
                  min="0"
                />
              </div>
            </div>
          </div>
        </div>
      )}

      {summary && (
        <>
          {/* Health Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div className="bg-white dark:bg-gray-800 rounded-lg p-4 border border-gray-200 dark:border-gray-700 shadow-sm">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-gray-500 dark:text-gray-400">Total Tables</span>
                <Database className="w-5 h-5 text-gray-400" />
              </div>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">
                {totalTables.toLocaleString()}
              </p>
            </div>

            <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4 border border-green-200 dark:border-green-800">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-green-600 dark:text-green-400">Healthy</span>
                <CheckCircle className="w-5 h-5 text-green-500" />
              </div>
              <p className="text-2xl font-bold text-green-700 dark:text-green-300">
                {okCount.toLocaleString()}
              </p>
              <p className="text-xs text-green-600 dark:text-green-400">{healthyPercent}% of total</p>
            </div>

            <div className="bg-yellow-50 dark:bg-yellow-900/20 rounded-lg p-4 border border-yellow-200 dark:border-yellow-800">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-yellow-600 dark:text-yellow-400">Warning</span>
                <AlertTriangle className="w-5 h-5 text-yellow-500" />
              </div>
              <p className="text-2xl font-bold text-yellow-700 dark:text-yellow-300">
                {summary.warning_tables.toLocaleString()}
              </p>
            </div>

            <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-4 border border-red-200 dark:border-red-800">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-red-600 dark:text-red-400">Critical</span>
                <XCircle className="w-5 h-5 text-red-500" />
              </div>
              <p className="text-2xl font-bold text-red-700 dark:text-red-300">
                {summary.critical_tables.toLocaleString()}
              </p>
            </div>
          </div>

          {/* Maintenance Needed */}
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 shadow-sm overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
              <h3 className="font-semibold text-gray-900 dark:text-white">Maintenance Needed</h3>
            </div>
            <div className="divide-y divide-gray-200 dark:divide-gray-700">
              <MaintenanceRow
                icon={<Trash2 className="w-5 h-5 text-purple-500" />}
                label="Snapshot Expiration"
                count={summary.tables_needing_snapshot_expiration}
                description="Tables with too many snapshots"
                onClick={() => onViewTables?.({ type: 'snapshots', value: thresholds.snapshot_warning_threshold })}
              />
              <MaintenanceRow
                icon={<FileStack className="w-5 h-5 text-blue-500" />}
                label="File Compaction"
                count={summary.tables_needing_compaction}
                description="Tables with many small files"
                onClick={() => onViewTables?.({ type: 'small_files', value: thresholds.small_file_warning_threshold })}
              />
              <MaintenanceRow
                icon={<Files className="w-5 h-5 text-orange-500" />}
                label="Manifest Rewrite"
                count={summary.tables_needing_manifest_rewrite}
                description="Tables with fragmented manifests"
                onClick={() => onViewTables?.({ type: 'manifests' })}
              />
              <MaintenanceRow
                icon={<XCircle className="w-5 h-5 text-red-500" />}
                label="Delete File Cleanup"
                count={summary.tables_with_delete_files}
                description="Tables with delete files affecting performance"
                onClick={() => onViewTables?.({ type: 'delete_files', value: thresholds.delete_file_warning_threshold })}
              />
            </div>
          </div>

          {/* Wasted Storage */}
          {summary.total_wasted_storage_gb > 0 && (
            <div className="mt-4 p-4 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg border border-yellow-200 dark:border-yellow-800">
              <div className="flex items-center gap-2">
                <AlertTriangle className="w-5 h-5 text-yellow-600 dark:text-yellow-400" />
                <p className="text-sm text-yellow-800 dark:text-yellow-200">
                  Estimated wasted storage from small files:{' '}
                  <span className="font-semibold">{summary.total_wasted_storage_gb.toFixed(2)} GB</span>
                </p>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

interface MaintenanceRowProps {
  icon: React.ReactNode;
  label: string;
  count: number;
  description: string;
  onClick?: () => void;
}

function MaintenanceRow({ icon, label, count, description, onClick }: MaintenanceRowProps) {
  return (
    <div className="flex items-center justify-between px-4 py-3 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors">
      <div className="flex items-center gap-3">
        {icon}
        <div>
          <p className="font-medium text-gray-900 dark:text-white">{label}</p>
          <p className="text-xs text-gray-500 dark:text-gray-400">{description}</p>
        </div>
      </div>
      <div className="flex items-center gap-3">
        <span className={`text-lg font-semibold ${count > 0 ? 'text-gray-900 dark:text-white' : 'text-gray-400'}`}>
          {count.toLocaleString()}
        </span>
        {count > 0 && onClick && (
          <button
            onClick={onClick}
            className="text-sm text-iceberg hover:text-iceberg/80 font-medium"
          >
            View Tables
          </button>
        )}
      </div>
    </div>
  );
}
