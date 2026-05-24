import { useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ChevronRight,
  ChevronDown,
  Database,
  Table,
  FolderOpen,
  Plus,
  Trash2,
  CheckCircle,
  XCircle,
  Loader2,
  Activity,
  Search,
} from 'lucide-react';
import { useCatalogs, useDeleteCatalog } from '../../hooks/useCatalog';
import { useNamespaces, useTables } from '../../hooks/useIcebergData';
import { useTableSearch } from '../../hooks/useTableSearch';
import { catalogHealthPath, tablePath } from '../../lib/paths';
import type { CatalogInfo } from '../../types/iceberg';

interface SidebarProps {
  onAddCatalog: () => void;
  selectedTable?: { catalog: string; namespace: string; table: string };
}

interface CatalogTreeItemProps {
  catalog: CatalogInfo;
  onDelete: (name: string) => void;
  selectedTable?: { catalog: string; namespace: string; table: string };
}

function CatalogTreeItem({
  catalog,
  onDelete,
  selectedTable,
}: CatalogTreeItemProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  // Only fetch namespaces when expanded (fast, no S3 calls)
  const { data: namespaces, isLoading } = useNamespaces(isExpanded ? catalog.name : '');

  return (
    <div className="select-none">
      <div className="flex items-center gap-2 px-2 py-1.5 hover:bg-gray-100 dark:hover:bg-gray-700 rounded cursor-pointer group">
        <button
          type="button"
          className="p-0 bg-transparent border-0 cursor-pointer"
          onClick={() => setIsExpanded(!isExpanded)}
        >
          {isExpanded ? (
            <ChevronDown className="w-4 h-4 text-gray-500" />
          ) : (
            <ChevronRight className="w-4 h-4 text-gray-500" />
          )}
        </button>
        <Database className="w-4 h-4 text-iceberg" />
        <Link
          to={catalogHealthPath(catalog.name)}
          className="flex-1 text-sm font-medium text-gray-700 dark:text-gray-200 truncate hover:text-iceberg"
        >
          {catalog.name}
        </Link>
        {catalog.connected ? (
          <CheckCircle className="w-4 h-4 text-green-500" />
        ) : (
          <XCircle className="w-4 h-4 text-red-500" />
        )}
        <Link
          to={catalogHealthPath(catalog.name)}
          className="p-1 hover:bg-iceberg/10 rounded transition-colors"
          title="Catalog Health Dashboard"
          onClick={(e) => e.stopPropagation()}
        >
          <Activity className="w-4 h-4 text-iceberg" />
        </Link>
        <button
          onClick={(e) => {
            e.stopPropagation();
            onDelete(catalog.name);
          }}
          className="opacity-0 group-hover:opacity-100 p-1 hover:bg-red-100 dark:hover:bg-red-900 rounded transition-opacity"
          title="Remove catalog"
        >
          <Trash2 className="w-3 h-3 text-red-500" />
        </button>
      </div>

      {isExpanded && (
        <div className="ml-4">
          {isLoading ? (
            <div className="flex items-center gap-2 px-2 py-1.5 text-gray-500">
              <Loader2 className="w-4 h-4 animate-spin" />
              <span className="text-sm">Loading namespaces...</span>
            </div>
          ) : !namespaces || namespaces.length === 0 ? (
            <div className="px-2 py-1.5 text-sm text-gray-500">No namespaces found</div>
          ) : (
            namespaces.map((namespace) => (
              <NamespaceTreeItem
                key={namespace}
                catalogName={catalog.name}
                namespace={namespace}
                selectedTable={selectedTable}
              />
            ))
          )}
        </div>
      )}
    </div>
  );
}

interface NamespaceTreeItemProps {
  catalogName: string;
  namespace: string;
  selectedTable?: { catalog: string; namespace: string; table: string };
}

function NamespaceTreeItem({
  catalogName,
  namespace,
  selectedTable,
}: NamespaceTreeItemProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  // Only fetch tables when namespace is expanded (lazy mode, no S3 calls)
  const { data: tables, isLoading } = useTables(
    isExpanded ? catalogName : '',
    { namespace, lazy: true }
  );

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
        <FolderOpen className="w-4 h-4 text-yellow-500" />
        <span className="text-sm text-gray-600 dark:text-gray-300">{namespace}</span>
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

              return (
                <Link
                  key={table.name}
                  to={tablePath(catalogName, namespace, table.name)}
                  className={`flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer ${
                    isSelected
                      ? 'bg-iceberg/10 text-iceberg'
                      : 'text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700'
                  }`}
                >
                  <Table className="w-4 h-4 shrink-0" />
                  <span className="text-sm truncate">{table.name}</span>
                  {table.snapshot_count !== null && table.snapshot_count !== undefined && (
                    <span className="text-xs text-gray-400 ml-auto">
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

export function Sidebar({ onAddCatalog, selectedTable }: SidebarProps) {
  const { data: catalogs, isLoading } = useCatalogs();
  const [searchQuery, setSearchQuery] = useState('');
  const tableSearch = useTableSearch(catalogs, searchQuery);
  const deleteCatalog = useDeleteCatalog();

  const handleDelete = (name: string) => {
    if (confirm(`Are you sure you want to remove catalog "${name}"?`)) {
      deleteCatalog.mutate(name);
    }
  };

  return (
    <aside className="w-64 bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 flex flex-col h-full">
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
                    className={`w-full flex items-start gap-2 px-2 py-2 rounded text-left ${
                      isSelected
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
        ) : isLoading ? (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
          </div>
        ) : catalogs?.length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            <Database className="w-12 h-12 mx-auto mb-2 opacity-50" />
            <p className="text-sm">No catalogs configured</p>
            <p className="text-xs mt-1">Click "Add Catalog" to get started</p>
          </div>
        ) : (
          catalogs?.map((catalog) => (
            <CatalogTreeItem
              key={catalog.name}
              catalog={catalog}
              onDelete={handleDelete}
              selectedTable={selectedTable}
            />
          ))
        )}
      </div>
    </aside>
  );
}
