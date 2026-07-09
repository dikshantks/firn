import { useState } from 'react';
import {
  ArrowLeft,
  ArrowUpDown,
  ChevronLeft,
  ChevronRight,
  Copy,
  CheckCircle,
  AlertTriangle,
  XCircle,
  Loader2,
  Download,
  Filter,
  RefreshCw,
} from 'lucide-react';
import { useCachedTables, useScanTableHealth } from '../../hooks/useHealth';
import type { CachedTableHealth } from '../../services/api';

interface MaintenanceTableProps {
  catalogName: string;
  filter?: {
    type: string;
    value?: number;
  };
  onBack?: () => void;
}

type SortField = 'health_score' | 'total_snapshots' | 'total_delete_files' | 'small_files_count' | 'total_size_gb';
type SortOrder = 'asc' | 'desc';

const PAGE_SIZE = 50;

export function MaintenanceTable({ catalogName, filter, onBack }: MaintenanceTableProps) {
  const [statusFilter, setStatusFilter] = useState<'healthy' | 'warning' | 'critical' | undefined>(undefined);
  const [minSnapshots, setMinSnapshots] = useState<number | undefined>(filter?.type === 'snapshots' ? filter.value : undefined);
  const [minDeleteFiles, setMinDeleteFiles] = useState<number | undefined>(filter?.type === 'delete_files' ? filter.value : undefined);
  const [minSmallFiles, setMinSmallFiles] = useState<number | undefined>(filter?.type === 'small_files' ? filter.value : undefined);
  const [page, setPage] = useState(0);
  const [sortField, setSortField] = useState<SortField>('health_score');
  const [sortOrder, setSortOrder] = useState<SortOrder>('asc');
  const [copiedCommand, setCopiedCommand] = useState<string | null>(null);
  const [confirmModal, setConfirmModal] = useState<{
    table: CachedTableHealth;
    type: 'expire' | 'compact' | 'rewrite';
    command: string;
  } | null>(null);
  const [confirmInput, setConfirmInput] = useState('');
  const [scanningTableKey, setScanningTableKey] = useState<string | null>(null);

  const { data: tables, isLoading, error, refetch } = useCachedTables(catalogName, {
    status_filter: statusFilter,
    min_snapshots: minSnapshots,
    min_delete_files: minDeleteFiles,
    min_small_files: minSmallFiles,
    limit: PAGE_SIZE,
    offset: page * PAGE_SIZE,
  });

  const scanTableHealth = useScanTableHealth();

  const handleScanTable = async (table: CachedTableHealth, mode: 'light' | 'full' = 'light') => {
    const tableKey = `${table.namespace}.${table.table_name}`;
    setScanningTableKey(tableKey);
    try {
      await scanTableHealth.mutateAsync({
        catalog: catalogName,
        namespace: table.namespace,
        table: table.table_name,
        mode,
      });
      await refetch();
    } catch (scanError) {
      console.error('Failed to scan table health:', scanError);
    } finally {
      setScanningTableKey(null);
    }
  };

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('desc');
    }
  };

  const sortedTables = tables ? [...tables].sort((a, b) => {
    const aVal = a[sortField] ?? 0;
    const bVal = b[sortField] ?? 0;
    return sortOrder === 'asc' ? aVal - bVal : bVal - aVal;
  }) : [];

  const buildCommand = (table: CachedTableHealth, type: 'expire' | 'compact' | 'rewrite'): string => {
    const fullTableName = `${table.namespace}.${table.table_name}`;
    switch (type) {
      case 'expire':
        return `CALL ${catalogName}.system.expire_snapshots('${fullTableName}', TIMESTAMP '${new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]} 00:00:00')`;
      case 'compact':
        return `CALL ${catalogName}.system.rewrite_data_files(table => '${fullTableName}', options => map('target-file-size-bytes', '536870912'))`;
      case 'rewrite':
        return `CALL ${catalogName}.system.rewrite_manifests('${fullTableName}')`;
    }
  };

  const openConfirmModal = (table: CachedTableHealth, type: 'expire' | 'compact' | 'rewrite') => {
    setConfirmInput('');
    setConfirmModal({ table, type, command: buildCommand(table, type) });
  };

  const handleConfirmCopy = () => {
    if (!confirmModal) return;
    navigator.clipboard.writeText(confirmModal.command);
    setCopiedCommand(`${confirmModal.table.table_name}-${confirmModal.type}`);
    setTimeout(() => setCopiedCommand(null), 2000);
    setConfirmModal(null);
  };


  const exportCsv = () => {
    if (!tables || tables.length === 0) return;
    
    const headers = ['Namespace', 'Table', 'Status', 'Health Score', 'Snapshots', 'Data Files', 'Delete Files', 'Small Files', 'Size (GB)'];
    const rows = tables.map(t => [
      t.namespace,
      t.table_name,
      t.status,
      t.health_score,
      t.total_snapshots,
      t.total_data_files,
      t.total_delete_files,
      t.small_files_count,
      t.total_size_gb.toFixed(2),
    ]);
    
    const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${catalogName}-health-${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'healthy':
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case 'warning':
        return <AlertTriangle className="w-4 h-4 text-yellow-500" />;
      case 'critical':
        return <XCircle className="w-4 h-4 text-red-500" />;
      default:
        return null;
    }
  };

  const getStatusBadge = (status: string) => {
    const colors = {
      healthy: 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400',
      warning: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400',
      critical: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400',
    };
    return colors[status as keyof typeof colors] || 'bg-gray-100 text-gray-800';
  };

  const getFilterTitle = () => {
    if (filter?.type === 'snapshots') return 'Tables Needing Snapshot Expiration';
    if (filter?.type === 'small_files') return 'Tables Needing Compaction';
    if (filter?.type === 'delete_files') return 'Tables with Delete Files';
    if (filter?.type === 'manifests') return 'Tables Needing Manifest Rewrite';
    return 'Cached Table Health';
  };

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center h-64">
        <Loader2 className="w-8 h-8 animate-spin text-iceberg mb-4" />
        <p className="text-gray-500">Loading cached tables...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-64 text-red-500">
        <XCircle className="w-8 h-8 mb-4" />
        <p>Failed to load cached tables</p>
        <p className="text-sm text-gray-500 mt-2">Make sure you have run a health scan first.</p>
      </div>
    );
  }

  return (
    <div className="p-6">
      {/* Confirmation Modal */}
      {confirmModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
          <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl border border-gray-200 dark:border-gray-700 w-full max-w-lg mx-4">
            <div className="flex items-center gap-3 px-6 py-4 border-b border-gray-200 dark:border-gray-700">
              <AlertTriangle className="w-5 h-5 text-yellow-500 shrink-0" />
              <h3 className="text-base font-semibold text-gray-900 dark:text-white">Confirm Maintenance Command</h3>
            </div>
            <div className="px-6 py-4 space-y-4">
              <p className="text-sm text-gray-600 dark:text-gray-400">
                This command will permanently alter table{' '}
                <span className="font-mono font-semibold text-gray-900 dark:text-white">
                  {confirmModal.table.namespace}.{confirmModal.table.table_name}
                </span>.
                Type the table name below to copy the command.
              </p>
              <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3 border border-gray-200 dark:border-gray-700">
                <p className="text-xs font-mono text-gray-700 dark:text-gray-300 break-all">{confirmModal.command}</p>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
                  Type <span className="font-mono font-semibold text-gray-800 dark:text-gray-200">{confirmModal.table.table_name}</span> to confirm
                </label>
                <input
                  type="text"
                  autoFocus
                  value={confirmInput}
                  onChange={(e) => setConfirmInput(e.target.value)}
                  onKeyDown={(e) => { if (e.key === 'Enter' && confirmInput === confirmModal.table.table_name) handleConfirmCopy(); }}
                  placeholder={confirmModal.table.table_name}
                  className="w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 dark:text-white focus:outline-none focus:ring-2 focus:ring-yellow-400"
                />
              </div>
            </div>
            <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 dark:border-gray-700">
              <button
                onClick={() => setConfirmModal(null)}
                className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleConfirmCopy}
                disabled={confirmInput !== confirmModal.table.table_name}
                className="flex items-center gap-2 px-4 py-2 text-sm bg-yellow-500 hover:bg-yellow-600 disabled:bg-gray-300 dark:disabled:bg-gray-700 disabled:cursor-not-allowed text-white rounded-lg font-medium transition-colors"
              >
                <Copy className="w-4 h-4" />
                Copy & Confirm
              </button>
            </div>
          </div>
        </div>
      )}
      {onBack && (
        <button
          onClick={onBack}
          className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 mb-4"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Dashboard
        </button>
      )}

      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
            {getFilterTitle()}
          </h2>
          <p className="text-sm text-gray-500">{catalogName}</p>
        </div>
        <button
          onClick={exportCsv}
          disabled={!tables || tables.length === 0}
          className="flex items-center gap-2 px-3 py-2 text-sm bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50"
        >
          <Download className="w-4 h-4" />
          Export CSV
        </button>
      </div>

      {/* Filters */}
      <div className="mb-4 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg border border-gray-200 dark:border-gray-700">
        <div className="flex items-center gap-2 mb-3">
          <Filter className="w-4 h-4 text-gray-400" />
          <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Filters</span>
        </div>
        <div className="flex flex-wrap gap-4">
          <div>
            <label className="block text-xs text-gray-500 mb-1">Status</label>
            <select
              value={statusFilter || ''}
              onChange={(e) => {
                setStatusFilter(e.target.value as 'healthy' | 'warning' | 'critical' | undefined || undefined);
                setPage(0);
              }}
              className="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 dark:text-white"
            >
              <option value="">All</option>
              <option value="healthy">Healthy</option>
              <option value="warning">Warning</option>
              <option value="critical">Critical</option>
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Min Snapshots</label>
            <input
              type="number"
              value={minSnapshots || ''}
              onChange={(e) => {
                setMinSnapshots(e.target.value ? parseInt(e.target.value) : undefined);
                setPage(0);
              }}
              className="w-24 px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 dark:text-white"
              placeholder="0"
              min="0"
            />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Min Delete Files</label>
            <input
              type="number"
              value={minDeleteFiles || ''}
              onChange={(e) => {
                setMinDeleteFiles(e.target.value ? parseInt(e.target.value) : undefined);
                setPage(0);
              }}
              className="w-24 px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 dark:text-white"
              placeholder="0"
              min="0"
            />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Min Small Files</label>
            <input
              type="number"
              value={minSmallFiles || ''}
              onChange={(e) => {
                setMinSmallFiles(e.target.value ? parseInt(e.target.value) : undefined);
                setPage(0);
              }}
              className="w-24 px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 dark:text-white"
              placeholder="0"
              min="0"
            />
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 dark:bg-gray-700/50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Table
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Status
                </th>
                <SortableHeader
                  label="Score"
                  field="health_score"
                  currentField={sortField}
                  currentOrder={sortOrder}
                  onSort={handleSort}
                />
                <SortableHeader
                  label="Snapshots"
                  field="total_snapshots"
                  currentField={sortField}
                  currentOrder={sortOrder}
                  onSort={handleSort}
                />
                <SortableHeader
                  label="Delete Files"
                  field="total_delete_files"
                  currentField={sortField}
                  currentOrder={sortOrder}
                  onSort={handleSort}
                />
                <SortableHeader
                  label="Small Files"
                  field="small_files_count"
                  currentField={sortField}
                  currentOrder={sortOrder}
                  onSort={handleSort}
                />
                <SortableHeader
                  label="Size (GB)"
                  field="total_size_gb"
                  currentField={sortField}
                  currentOrder={sortOrder}
                  onSort={handleSort}
                />
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
              {sortedTables.length === 0 ? (
                <tr>
                  <td colSpan={8} className="px-4 py-8 text-center text-gray-500">
                    No tables found matching the filters
                  </td>
                </tr>
              ) : (
                sortedTables.map((table) => (
                  <tr key={`${table.namespace}.${table.table_name}`} className="hover:bg-gray-50 dark:hover:bg-gray-700/30">
                    <td className="px-4 py-3">
                      <div>
                        <p className="font-medium text-gray-900 dark:text-white text-sm">{table.table_name}</p>
                        <p className="text-xs text-gray-500">{table.namespace}</p>
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium ${getStatusBadge(table.status)}`}>
                        {getStatusIcon(table.status)}
                        {table.status}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-900 dark:text-white font-medium">
                      {table.health_score}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {table.total_snapshots.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {table.total_delete_files.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {table.small_files_count.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {table.total_size_gb.toFixed(2)}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-1">
                        <button
                          type="button"
                          onClick={() => void handleScanTable(table, 'light')}
                          disabled={scanningTableKey === `${table.namespace}.${table.table_name}`}
                          title="Re-scan this table (light)"
                          className="px-2 py-1 text-xs bg-iceberg/10 text-iceberg rounded hover:bg-iceberg/20 flex items-center gap-1 disabled:opacity-50"
                        >
                          {scanningTableKey === `${table.namespace}.${table.table_name}` ? (
                            <Loader2 className="w-3 h-3 animate-spin" />
                          ) : (
                            <RefreshCw className="w-3 h-3" />
                          )}
                          Scan
                        </button>
                        {table.total_snapshots > 50 && (
                          <ActionButton
                            label="Expire"
                            onClick={() => openConfirmModal(table, 'expire')}
                            copied={copiedCommand === `${table.table_name}-expire`}
                          />
                        )}
                        {table.small_files_count > 0 && (
                          <ActionButton
                            label="Compact"
                            onClick={() => openConfirmModal(table, 'compact')}
                            copied={copiedCommand === `${table.table_name}-compact`}
                          />
                        )}
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {tables && tables.length > 0 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200 dark:border-gray-700">
            <p className="text-sm text-gray-500">
              Showing {page * PAGE_SIZE + 1} - {page * PAGE_SIZE + tables.length}
            </p>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPage(Math.max(0, page - 1))}
                disabled={page === 0}
                className="p-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50"
              >
                <ChevronLeft className="w-5 h-5" />
              </button>
              <span className="text-sm text-gray-600 dark:text-gray-400">Page {page + 1}</span>
              <button
                onClick={() => setPage(page + 1)}
                disabled={tables.length < PAGE_SIZE}
                className="p-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50"
              >
                <ChevronRight className="w-5 h-5" />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

interface SortableHeaderProps {
  label: string;
  field: SortField;
  currentField: SortField;
  currentOrder: SortOrder;
  onSort: (field: SortField) => void;
}

function SortableHeader({ label, field, currentField, currentOrder: _currentOrder, onSort }: SortableHeaderProps) {
  const isActive = currentField === field;
  return (
    <th
      className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600"
      onClick={() => onSort(field)}
    >
      <div className="flex items-center gap-1">
        {label}
        <ArrowUpDown className={`w-3 h-3 ${isActive ? 'text-iceberg' : 'text-gray-400'}`} />
      </div>
    </th>
  );
}

interface ActionButtonProps {
  label: string;
  onClick: () => void;
  copied: boolean;
}

function ActionButton({ label, onClick, copied }: ActionButtonProps) {
  return (
    <button
      onClick={onClick}
      className="px-2 py-1 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 dark:hover:bg-gray-600 flex items-center gap-1"
    >
      {copied ? <CheckCircle className="w-3 h-3 text-green-500" /> : <Copy className="w-3 h-3" />}
      {copied ? 'Copied!' : label}
    </button>
  );
}
