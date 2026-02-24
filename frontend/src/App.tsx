import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { AppShell } from './components/Layout/AppShell';
import { CatalogForm } from './components/Catalog/CatalogForm';
import { SnapshotDAG } from './components/Visualization/SnapshotDAG';
import { ManifestTree } from './components/Visualization/ManifestTree';
import { DataFileTable } from './components/Visualization/DataFileTable';
import { PuffinViewer } from './components/Visualization/PuffinViewer';
import { SnapshotDetail } from './components/Details/SnapshotDetail';
import { CompareSnapshots } from './components/Details/CompareSnapshots';
import { DataFileDetail } from './components/Details/DataFileDetail';
import { StorageStats } from './components/Analytics/StorageStats';
import { OperationTimeline } from './components/Analytics/OperationTimeline';
import { CatalogHealthDashboard } from './components/Health/CatalogHealthDashboard';
import { useTableMetadata } from './hooks/useIcebergData';
import type { SnapshotInfo, DataFileInfo, ManifestEntry } from './types/iceberg';
import {
  Database,
  GitBranch,
  FileText,
  BarChart3,
  Clock,
  File,
  Layers,
} from 'lucide-react';

type ViewTab = 'snapshots' | 'manifests' | 'files' | 'statistics' | 'analytics' | 'timeline';

function App() {
  const queryClient = useQueryClient();
  
  // State
  const [showCatalogForm, setShowCatalogForm] = useState(false);
  const [selectedCatalogForHealth, setSelectedCatalogForHealth] = useState<string | undefined>();
  const [selectedTable, setSelectedTable] = useState<{
    catalog: string;
    namespace: string;
    table: string;
  } | undefined>();
  const [selectedSnapshot, setSelectedSnapshot] = useState<SnapshotInfo | undefined>();
  const [compareSnapshots, setCompareSnapshots] = useState<{
    snapshot1: SnapshotInfo;
    snapshot2: SnapshotInfo;
  } | undefined>();
  const [selectedFile, setSelectedFile] = useState<string | undefined>();
  const [activeTab, setActiveTab] = useState<ViewTab>('snapshots');

  // Fetch table metadata when a table is selected
  const { data: tableMetadata } = useTableMetadata(
    selectedTable?.catalog || '',
    selectedTable?.namespace || '',
    selectedTable?.table || ''
  );

  const handleTableSelect = (catalog: string, namespace: string, table: string) => {
    setSelectedTable({ catalog, namespace, table });
    setSelectedCatalogForHealth(undefined);
    setSelectedSnapshot(undefined);
    setCompareSnapshots(undefined);
    setSelectedFile(undefined);
    setActiveTab('snapshots');
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
    setActiveTab('files');
  };

  const handleRefresh = () => {
    queryClient.invalidateQueries();
  };

  const tabs: { id: ViewTab; label: string; icon: React.ReactNode }[] = [
    { id: 'snapshots', label: 'Snapshots', icon: <GitBranch className="w-4 h-4" /> },
    { id: 'manifests', label: 'Manifests', icon: <Layers className="w-4 h-4" /> },
    { id: 'files', label: 'Files', icon: <File className="w-4 h-4" /> },
    { id: 'statistics', label: 'Statistics', icon: <BarChart3 className="w-4 h-4" /> },
    { id: 'analytics', label: 'Storage', icon: <FileText className="w-4 h-4" /> },
    { id: 'timeline', label: 'Timeline', icon: <Clock className="w-4 h-4" /> },
  ];

  return (
    <>
      <AppShell
        onTableSelect={handleTableSelect}
        onAddCatalog={() => setShowCatalogForm(true)}
        onCatalogHealthClick={(catalog) => setSelectedCatalogForHealth(catalog)}
        onRefresh={handleRefresh}
        selectedTable={selectedTable}
      >
        {selectedCatalogForHealth ? (
          <div className="h-full overflow-auto bg-white dark:bg-gray-800">
            <CatalogHealthDashboard
              catalogName={selectedCatalogForHealth}
              onBack={() => setSelectedCatalogForHealth(undefined)}
            />
          </div>
        ) : !selectedTable ? (
          // Welcome screen
          <div className="flex items-center justify-center h-full">
            <div className="text-center max-w-md">
              <Database className="w-20 h-20 mx-auto text-iceberg mb-6" />
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-3">
                Welcome to Iceberg Visualizer
              </h2>
              <p className="text-gray-500 dark:text-gray-400 mb-6">
                Connect to a catalog and select a table from the sidebar to explore
                its metadata, snapshots, manifests, and statistics.
              </p>
              <button
                onClick={() => setShowCatalogForm(true)}
                className="px-6 py-3 bg-iceberg text-white rounded-lg hover:bg-iceberg-dark transition-colors font-medium"
              >
                Add Your First Catalog
              </button>
            </div>
          </div>
        ) : selectedTable ? (
          // Table view
          <div className="flex flex-col h-full">
            {/* Table header */}
            <div className="px-4 py-3 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
                    {selectedTable.namespace}.{selectedTable.table}
                  </h2>
                  <p className="text-sm text-gray-500">
                    {selectedTable.catalog} | Format v{tableMetadata?.format_version || '?'} |{' '}
                    {tableMetadata?.snapshot_count || 0} snapshots
                  </p>
                </div>
              </div>

              {/* Tabs */}
              <div className="flex gap-1 mt-3 -mb-3">
                {tabs.map((tab) => (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${
                      activeTab === tab.id
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

            {/* Tab content */}
            <div className="flex-1 overflow-hidden">
              {activeTab === 'snapshots' && (
                <div className="flex h-full">
                  <div className="flex-1 border-r border-gray-200 dark:border-gray-700">
                    <SnapshotDAG
                      catalog={selectedTable.catalog}
                      namespace={selectedTable.namespace}
                      table={selectedTable.table}
                      onSnapshotSelect={handleSnapshotSelect}
                      onCompareSelect={handleCompareSelect}
                    />
                  </div>
                  <div className="w-80 overflow-y-auto bg-white dark:bg-gray-800">
                    {compareSnapshots ? (
                      <CompareSnapshots
                        catalog={selectedTable.catalog}
                        namespace={selectedTable.namespace}
                        table={selectedTable.table}
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
                    catalog={selectedTable.catalog}
                    namespace={selectedTable.namespace}
                    table={selectedTable.table}
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
                        catalog={selectedTable.catalog}
                        namespace={selectedTable.namespace}
                        table={selectedTable.table}
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
                        catalog={selectedTable.catalog}
                        namespace={selectedTable.namespace}
                        table={selectedTable.table}
                        filePath={selectedFile}
                      />
                    </div>
                  )}
                </div>
              )}

              {activeTab === 'statistics' && (
                <div className="h-full overflow-y-auto bg-white dark:bg-gray-800">
                  <PuffinViewer
                    catalog={selectedTable.catalog}
                    namespace={selectedTable.namespace}
                    table={selectedTable.table}
                    snapshotId={selectedSnapshot?.snapshot_id}
                  />
                </div>
              )}

              {activeTab === 'analytics' && (
                <div className="h-full overflow-y-auto bg-white dark:bg-gray-800">
                  <StorageStats
                    catalog={selectedTable.catalog}
                    namespace={selectedTable.namespace}
                    table={selectedTable.table}
                  />
                </div>
              )}

              {activeTab === 'timeline' && (
                <div className="h-full overflow-y-auto bg-white dark:bg-gray-800">
                  <OperationTimeline
                    catalog={selectedTable.catalog}
                    namespace={selectedTable.namespace}
                    table={selectedTable.table}
                  />
                </div>
              )}
            </div>
          </div>
        ) : null}
      </AppShell>

      {/* Catalog form modal */}
      {showCatalogForm && (
        <CatalogForm onClose={() => setShowCatalogForm(false)} />
      )}
    </>
  );
}

export default App;
