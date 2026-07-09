import { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import {
  ChevronRight,
  ChevronDown,
  Database,
  Table,
  FolderOpen,
  Plus,
  Loader2,
  Search,
} from 'lucide-react';
import { useCatalogs } from '../../hooks/useCatalog';
import { useNamespaces, useTables } from '../../hooks/useIcebergData';
import { useTableSearch } from '../../hooks/useTableSearch';
import { useCachedTables } from '../../hooks/useHealth';
import { tablePath } from '../../lib/paths';
import type { CachedTableHealth } from '../../services/api';

interface SidebarProps {
  onAddCatalog: () => void;
  selectedTable?: { catalog: string; namespace: string; table: string };
  width?: number;
}

const getMaintenanceColor = (tableHealth: CachedTableHealth): string | null => {
  if (tableHealth.status === 'healthy') return null;
  if (tableHealth.total_delete_files >= 10) {
    return 'bg-red-500'; // red for delete files / manifest issues
  }
  if (tableHealth.small_files_count >= 100) {
    return 'bg-blue-500'; // blue for compaction
  }
  if (tableHealth.total_snapshots >= 50 || (tableHealth.oldest_snapshot_age_days && tableHealth.oldest_snapshot_age_days >= 30)) {
    return 'bg-purple-500'; // purple for snapshot expiration
  }
  if (tableHealth.status === 'critical') return 'bg-red-500';
  if (tableHealth.status === 'warning') return 'bg-yellow-500';
  return null;
};

const getNamespaceColor = (tables: CachedTableHealth[]): string | null => {
  const colors = tables.map(getMaintenanceColor).filter(Boolean);
  if (colors.includes('bg-red-500')) return 'bg-red-500';
  if (colors.includes('bg-blue-500')) return 'bg-blue-500';
  if (colors.includes('bg-purple-500')) return 'bg-purple-500';
  if (colors.includes('bg-yellow-500')) return 'bg-yellow-500';
  return null;
};

interface NamespaceTreeItemProps {
  catalogName: string;
  namespace: string;
  selectedTable?: { catalog: string; namespace: string; table: string };
  cachedTables: CachedTableHealth[];
}

function NamespaceTreeItem({
  catalogName,
  namespace,
  selectedTable,
  cachedTables,
}: NamespaceTreeItemProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  // Only fetch tables when namespace is expanded (lazy mode, no S3 calls)
  const { data: tables, isLoading } = useTables(
    isExpanded ? catalogName : '',
    { namespace, lazy: true }
  );

  const namespaceTables = cachedTables.filter(t => t.namespace === namespace);
  const namespaceDotColor = getNamespaceColor(namespaceTables);

  return (
    <div>
      <div
        className="flex items-center gap-2 px-2 py-1.5 hover:bg-gray-100 dark:hover:bg-gray-700 rounded cursor-pointer"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        {isExpanded ? (
          <ChevronDown className="w-4 h-4 text-gray-500" />
        ) : (
          <ChevronRight className="w-4 h-4 text-gray-500" />
        )}
        <FolderOpen className="w-4 h-4 text-yellow-500 shrink-0" />
        {namespaceDotColor && (
          <span className={`w-2 h-2 rounded-full ${namespaceDotColor} shrink-0`} title="Maintenance Needed" />
        )}
        <span className="text-sm text-gray-600 dark:text-gray-300 truncate">{namespace}</span>
        {tables && <span className="text-xs text-gray-400">({tables.length})</span>}
      </div>

      {isExpanded && (
        <div className="ml-4">
          {isLoading ? (
            <div className="flex items-center gap-2 px-2 py-1.5 text-gray-500">
              <Loader2 className="w-4 h-4 animate-spin" />
              <span className="text-sm">Loading tables...</span>
            </div>
          ) : !tables || tables.length === 0 ? (
            <div className="px-2 py-1.5 text-sm text-gray-500">No tables found</div>
          ) : (
            tables.map((table) => {
              const isSelected =
                selectedTable?.catalog === catalogName &&
                selectedTable?.namespace === namespace &&
                selectedTable?.table === table.name;

              const tableHealth = namespaceTables.find(t => t.table_name === table.name);
              const tableDotColor = tableHealth ? getMaintenanceColor(tableHealth) : null;

              return (
                <Link
                  key={table.name}
                  to={tablePath(catalogName, namespace, table.name)}
                  className={`flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer ${isSelected
                      ? 'bg-iceberg/10 text-iceberg'
                      : 'text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700'
                    }`}
                >
                  <Table className="w-4 h-4 shrink-0" />
                  {tableDotColor && (
                    <span className={`w-2 h-2 rounded-full ${tableDotColor} shrink-0`} title="Maintenance Needed" />
                  )}
                  <span className="text-sm truncate">{table.name}</span>
                  {table.snapshot_count !== null && table.snapshot_count !== undefined && (
                    <span className="text-xs text-gray-400 ml-auto shrink-0">
                      {table.snapshot_count} snaps
                    </span>
                  )}
                </Link>
              );
            })
          )}
        </div>
      )}
    </div>
  );
}

export function Sidebar({ onAddCatalog, selectedTable, width = 256 }: SidebarProps) {
  const { data: catalogs, isLoading } = useCatalogs();
  const { catalog: currentCatalogName } = useParams();
  const activeCatalogName = currentCatalogName || catalogs?.[0]?.name || '';
  const { data: namespaces, isLoading: isNamespacesLoading } = useNamespaces(activeCatalogName);
  const { data: cachedTables } = useCachedTables(activeCatalogName);

  const [searchQuery, setSearchQuery] = useState('');
  const tableSearch = useTableSearch(catalogs, searchQuery);

  return (
    <aside
      style={{ width: `${width}px` }}
      className="bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 flex flex-col h-full shrink-0"
    >
      <div className="p-3 border-b border-gray-200 dark:border-gray-700">
        <button
          onClick={onAddCatalog}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-iceberg text-white rounded-lg hover:bg-iceberg-dark transition-colors"
        >
          <Plus className="w-4 h-4" />
          <span className="text-sm font-medium">Add Catalog</span>
        </button>
        <div className="relative mt-3">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            placeholder="Search table or schema.table"
            className="w-full pl-9 pr-3 py-2 text-sm rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-iceberg"
          />
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-2">
        {tableSearch.shouldSearch ? (
          <div className="space-y-1">
            {tableSearch.isSearching && (
              <div className="flex items-center gap-2 px-2 py-2 text-gray-500">
                <Loader2 className="w-4 h-4 animate-spin" />
                <span className="text-sm">Searching tables...</span>
              </div>
            )}
            {!tableSearch.isSearching && tableSearch.results.length === 0 ? (
              <div className="px-2 py-6 text-center text-sm text-gray-500">
                No tables matched "{searchQuery.trim()}"
              </div>
            ) : (
              tableSearch.results.map((result) => {
                const isSelected =
                  selectedTable?.catalog === result.catalog &&
                  selectedTable?.namespace === result.namespace &&
                  selectedTable?.table === result.table;

                return (
                  <Link
                    key={`${result.catalog}.${result.namespace}.${result.table}`}
                    to={tablePath(result.catalog, result.namespace, result.table)}
                    className={`w-full flex items-start gap-2 px-2 py-2 rounded text-left ${isSelected
                        ? 'bg-iceberg/10 text-iceberg'
                        : 'hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-200'
                      }`}
                  >
                    <Table className="w-4 h-4 mt-0.5 shrink-0" />
                    <span className="min-w-0">
                      <span className="block text-sm font-medium truncate">{result.table}</span>
                      <span className="block text-xs text-gray-500 truncate">
                        {result.catalog} / {result.namespace}
                      </span>
                    </span>
                  </Link>
                );
              })
            )}
          </div>
        ) : (isLoading || isNamespacesLoading) ? (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
          </div>
        ) : catalogs?.length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            <Database className="w-12 h-12 mx-auto mb-2 opacity-50" />
            <p className="text-sm">No catalogs configured</p>
            <p className="text-xs mt-1">Click "Add Catalog" to get started</p>
          </div>
        ) : !namespaces || namespaces.length === 0 ? (
          <div className="px-2 py-6 text-center text-sm text-gray-500">
            No namespaces found
          </div>
        ) : (
          namespaces.map((namespace) => (
            <NamespaceTreeItem
              key={namespace}
              catalogName={activeCatalogName}
              namespace={namespace}
              selectedTable={selectedTable}
              cachedTables={cachedTables || []}
            />
          ))
        )}
      </div>
    </aside>
  );
}
