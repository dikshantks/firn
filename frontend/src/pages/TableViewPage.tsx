import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import {
  GitBranch,
  File,
  BarChart3,
  Clock,
  FileText,
  Layers,
  Wrench,
  RefreshCw,
  Loader2,
} from 'lucide-react';
import { SnapshotDAG } from '../components/Visualization/SnapshotDAG';
import { ManifestTree } from '../components/Visualization/ManifestTree';
import { DataFileTable } from '../components/Visualization/DataFileTable';
import { PuffinViewer } from '../components/Visualization/PuffinViewer';
import { SnapshotDetail } from '../components/Details/SnapshotDetail';
import { CompareSnapshots } from '../components/Details/CompareSnapshots';
import { DataFileDetail } from '../components/Details/DataFileDetail';
import { StorageStats } from '../components/Analytics/StorageStats';
import { OperationTimeline } from '../components/Analytics/OperationTimeline';
import { TableOptimizationSuggestions } from '../components/Health/TableOptimizationSuggestions';
import { useSnapshotGraph, useTableMetadata } from '../hooks/useIcebergData';
import { useCachedTableHealth, useScanTableHealth, useTableHealth } from '../hooks/useHealth';
import {
  appTabFromPathTab,
  decodeSegment,
  tabPathFromAppTab,
  type AppViewTab,
} from '../lib/paths';
import type { SnapshotInfo, DataFileInfo, ManifestEntry } from '../types/iceberg';

export function TableViewPage() {
  const navigate = useNavigate();
  const { catalog: catalogParam, namespace: namespaceParam, table: tableParam, tab: tabParam } =
    useParams();

  const catalog = decodeSegment(catalogParam) ?? '';
  const namespace = decodeSegment(namespaceParam) ?? '';
  const table = decodeSegment(tableParam) ?? '';
  const activeTab = appTabFromPathTab(tabParam);

  const [selectedSnapshot, setSelectedSnapshot] = useState<SnapshotInfo | undefined>();
  const [compareSnapshots, setCompareSnapshots] = useState<{
    snapshot1: SnapshotInfo;
    snapshot2: SnapshotInfo;
  } | undefined>();
  const [selectedFile, setSelectedFile] = useState<string | undefined>();

  const { data: tableMetadata } = useTableMetadata(catalog, namespace, table);
  const { data: snapshotGraph } = useSnapshotGraph(catalog, namespace, table);
  const { data: cachedTableHealth, refetch: refetchCachedHealth } = useCachedTableHealth(catalog, namespace, table);
  const { data: tableHealth } = useTableHealth(catalog, namespace, table, Boolean(cachedTableHealth));
  const scanTableHealth = useScanTableHealth();
  const [isScanningHealth, setIsScanningHealth] = useState(false);
  const hasHealthData = Boolean(cachedTableHealth);

  useEffect(() => {
    setSelectedSnapshot(undefined);
    setCompareSnapshots(undefined);
    setSelectedFile(undefined);
  }, [catalog, namespace, table]);

  useEffect(() => {
    if (activeTab === 'optimization' && !hasHealthData) {
      navigate(tabPathFromAppTab(catalog, namespace, table, 'analytics'), { replace: true });
    }
  }, [activeTab, hasHealthData, catalog, namespace, table, navigate]);

  useEffect(() => {
    if (!snapshotGraph?.nodes.length) {
      return;
    }

    const latestSnapshot =
      snapshotGraph.nodes.find((snapshot) => snapshot.snapshot_id === snapshotGraph.current_snapshot_id) ??
      [...snapshotGraph.nodes].sort((a, b) => b.timestamp_ms - a.timestamp_ms)[0];

    if (!latestSnapshot) {
      return;
    }

    setSelectedSnapshot((current) =>
      current && snapshotGraph.nodes.some((snapshot) => snapshot.snapshot_id === current.snapshot_id)
        ? current
        : latestSnapshot
    );
  }, [snapshotGraph]);

  const handleTabChange = (tab: AppViewTab) => {
    navigate(tabPathFromAppTab(catalog, namespace, table, tab));
  };

  const handleSnapshotSelect = (snapshot: SnapshotInfo) => {
    setSelectedSnapshot(snapshot);
    setCompareSnapshots(undefined);
  };

  const handleCompareSelect = (snapshot1: SnapshotInfo, snapshot2: SnapshotInfo) => {
    setCompareSnapshots({ snapshot1, snapshot2 });
  };

  const handleFileSelect = (file: DataFileInfo | ManifestEntry) => {
    setSelectedFile(file.file_path);
    handleTabChange('files');
  };

  const handleScanHealth = async (mode: 'light' | 'full' = 'light') => {
    setIsScanningHealth(true);
    try {
      await scanTableHealth.mutateAsync({ catalog, namespace, table, mode });
      await refetchCachedHealth();
    } catch (scanError) {
      console.error('Failed to scan table health:', scanError);
    } finally {
      setIsScanningHealth(false);
    }
  };

  const tabs: { id: AppViewTab; label: string; icon: ReactNode }[] = [
    { id: 'analytics', label: 'Storage', icon: <FileText className="w-4 h-4" /> },
    { id: 'snapshots', label: 'Snapshots', icon: <GitBranch className="w-4 h-4" /> },
    { id: 'manifests', label: 'Manifests', icon: <Layers className="w-4 h-4" /> },
    { id: 'files', label: 'Files', icon: <File className="w-4 h-4" /> },
    { id: 'statistics', label: 'Statistics', icon: <BarChart3 className="w-4 h-4" /> },
    { id: 'timeline', label: 'Timeline', icon: <Clock className="w-4 h-4" /> },
    ...(hasHealthData
      ? [{ id: 'optimization' as const, label: 'Optimize', icon: <Wrench className="w-4 h-4" /> }]
      : []),
  ];

  if (!catalog || !namespace || !table) {
    return null;
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-4 py-3 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
              {namespace}.{table}
            </h2>
            <p className="text-sm text-gray-500">
              {catalog} | Format v{tableMetadata?.format_version || '?'} |{' '}
              {tableMetadata?.snapshot_count || 0} snapshots
              {tableHealth && (
                <span className="ml-2">
                  | Health {tableHealth.health_score}/100 ({tableHealth.status})
                </span>
              )}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => void handleScanHealth('light')}
              disabled={isScanningHealth}
              className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-iceberg bg-iceberg/10 hover:bg-iceberg/20 rounded-lg transition-colors disabled:opacity-50"
            >
              {isScanningHealth ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <RefreshCw className="w-4 h-4" />
              )}
              {hasHealthData ? 'Re-scan Health' : 'Scan Health'}
            </button>
          </div>
        </div>

        {tableHealth && (
          <div className="mt-3 flex flex-wrap gap-2 text-xs">
            {tableHealth.metrics.total_records > 0 && (
              <span className="px-2 py-1 rounded bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border border-blue-150 dark:border-blue-800">
                <span className="font-semibold">{tableHealth.metrics.total_records.toLocaleString()}</span> rows
              </span>
            )}

            {(tableHealth.metrics.total_position_deletes > 0 || tableHealth.metrics.total_equality_deletes > 0) && (
              <span className="px-2 py-1 rounded bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-300 border border-red-150 dark:border-red-800 flex items-center gap-1">
                Delete records:{' '}
                {tableHealth.metrics.total_position_deletes > 0 && (
                  <span className="font-semibold">{tableHealth.metrics.total_position_deletes.toLocaleString()} pos</span>
                )}
                {tableHealth.metrics.total_equality_deletes > 0 && (
                  <span>
                    {tableHealth.metrics.total_position_deletes > 0 ? ', ' : ''}
                    <span className="font-semibold">{tableHealth.metrics.total_equality_deletes.toLocaleString()} eq</span>
                  </span>
                )}
              </span>
            )}

            {tableHealth.metrics.schema_evolution_count > 1 && (
              <span className="px-2 py-1 rounded bg-purple-50 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 border border-purple-150 dark:border-purple-800" title="Schema has evolved over time">
                Schema: <span className="font-semibold">{tableHealth.metrics.schema_evolution_count} versions</span>
              </span>
            )}

            {tableHealth.metrics.partition_spec_evolution_count > 1 && (
              <span className="px-2 py-1 rounded bg-yellow-50 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300 border border-yellow-250 dark:border-yellow-800" title="Partition spec has evolved, which can affect query performance">
                Partition specs: <span className="font-semibold">{tableHealth.metrics.partition_spec_evolution_count} versions</span>
              </span>
            )}

            {tableHealth.metrics.metadata_log_depth > 15 && (
              <span className="px-2 py-1 rounded bg-orange-50 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300 border border-orange-150 dark:border-orange-850" title="Metadata log depth is high. Suggests snapshot expiration or metadata cleanup is needed.">
                Metadata log: <span className="font-semibold">{tableHealth.metrics.metadata_log_depth} files</span>
              </span>
            )}
          </div>
        )}

        <div className="flex gap-1 mt-3 -mb-3">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              type="button"
              onClick={() => handleTabChange(tab.id)}
              className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${activeTab === tab.id
                ? 'bg-gray-100 dark:bg-gray-700 text-iceberg border-b-2 border-iceberg'
                : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-50 dark:hover:bg-gray-700'
                }`}
            >
              {tab.icon}
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 overflow-hidden">
        {activeTab === 'snapshots' && (
          <div className="flex h-full">
            <div className="flex-1 border-r border-gray-200 dark:border-gray-700">
              <SnapshotDAG
                catalog={catalog}
                namespace={namespace}
                table={table}
                selectedSnapshotId={selectedSnapshot?.snapshot_id}
                onSnapshotSelect={handleSnapshotSelect}
                onCompareSelect={handleCompareSelect}
              />
            </div>
            <div className="w-80 overflow-y-auto bg-white dark:bg-gray-800">
              {compareSnapshots ? (
                <CompareSnapshots
                  catalog={catalog}
                  namespace={namespace}
                  table={table}
                  snapshot1={compareSnapshots.snapshot1}
                  snapshot2={compareSnapshots.snapshot2}
                />
              ) : selectedSnapshot ? (
                <SnapshotDetail snapshot={selectedSnapshot} />
              ) : (
                <div className="flex items-center justify-center h-full text-gray-500 text-sm">
                  Select a snapshot to view details
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'manifests' && selectedSnapshot && (
          <div className="h-full overflow-y-auto bg-white dark:bg-gray-800">
            <ManifestTree
              catalog={catalog}
              namespace={namespace}
              table={table}
              snapshotId={selectedSnapshot.snapshot_id}
              onFileSelect={handleFileSelect}
            />
          </div>
        )}

        {activeTab === 'manifests' && !selectedSnapshot && (
          <div className="flex items-center justify-center h-full text-gray-500">
            <div className="text-center">
              <Layers className="w-12 h-12 mx-auto mb-4 opacity-50" />
              <p>Select a snapshot from the Snapshots tab to view its manifests</p>
            </div>
          </div>
        )}

        {activeTab === 'files' && (
          <div className="flex h-full">
            <div className="flex-1 overflow-y-auto bg-white dark:bg-gray-800">
              {selectedSnapshot ? (
                <DataFileTable
                  catalog={catalog}
                  namespace={namespace}
                  table={table}
                  snapshotId={selectedSnapshot.snapshot_id}
                  onFileSelect={(file) => setSelectedFile(file.file_path)}
                />
              ) : (
                <div className="flex items-center justify-center h-full text-gray-500">
                  <div className="text-center">
                    <File className="w-12 h-12 mx-auto mb-4 opacity-50" />
                    <p>Select a snapshot from the Snapshots tab to view its files</p>
                  </div>
                </div>
              )}
            </div>
            {selectedFile && (
              <div className="w-96 border-l border-gray-200 dark:border-gray-700 overflow-y-auto bg-white dark:bg-gray-800">
                <DataFileDetail
                  catalog={catalog}
                  namespace={namespace}
                  table={table}
                  filePath={selectedFile}
                />
              </div>
            )}
          </div>
        )}

        {activeTab === 'statistics' && (
          <div className="h-full overflow-y-auto bg-white dark:bg-gray-800">
            <PuffinViewer
              catalog={catalog}
              namespace={namespace}
              table={table}
              snapshotId={selectedSnapshot?.snapshot_id}
            />
          </div>
        )}

        {activeTab === 'analytics' && (
          <div className="h-full overflow-y-auto bg-white dark:bg-gray-800">
            <StorageStats catalog={catalog} namespace={namespace} table={table} />
          </div>
        )}

        {activeTab === 'timeline' && (
          <div className="h-full overflow-y-auto bg-white dark:bg-gray-800">
            <OperationTimeline catalog={catalog} namespace={namespace} table={table} />
          </div>
        )}

        {activeTab === 'optimization' && hasHealthData && (
          <div className="h-full overflow-y-auto bg-white dark:bg-gray-800">
            <TableOptimizationSuggestions catalog={catalog} namespace={namespace} table={table} />
          </div>
        )}
      </div>
    </div>
  );
}
