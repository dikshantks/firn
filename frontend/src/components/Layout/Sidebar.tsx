import { useState } from 'react';
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
} from 'lucide-react';
import { useCatalogs, useDeleteCatalog } from '../../hooks/useCatalog';
import { useTables } from '../../hooks/useIcebergData';
import type { CatalogInfo, TableInfo } from '../../types/iceberg';

interface SidebarProps {
  onTableSelect: (catalog: string, namespace: string, table: string) => void;
  onAddCatalog: () => void;
  onCatalogHealthClick?: (catalog: string) => void;
  selectedTable?: { catalog: string; namespace: string; table: string };
}

interface CatalogTreeItemProps {
  catalog: CatalogInfo;
  onTableSelect: (catalog: string, namespace: string, table: string) => void;
  onDelete: (name: string) => void;
  onCatalogHealthClick?: (catalog: string) => void;
  selectedTable?: { catalog: string; namespace: string; table: string };
}

function CatalogTreeItem({
  catalog,
  onTableSelect,
  onDelete,
  onCatalogHealthClick,
  selectedTable,
}: CatalogTreeItemProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const { data: tables, isLoading } = useTables(isExpanded ? catalog.name : '');

  // Group tables by namespace
  const tablesByNamespace = tables?.reduce((acc, table) => {
    if (!acc[table.namespace]) {
      acc[table.namespace] = [];
    }
    acc[table.namespace].push(table);
    return acc;
  }, {} as Record<string, TableInfo[]>) ?? {};

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
        <span
          className="flex-1 text-sm font-medium text-gray-700 dark:text-gray-200 truncate"
          role="button"
          tabIndex={0}
          onClick={(e) => {
            e.stopPropagation();
            onCatalogHealthClick?.(catalog.name);
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter' || e.key === ' ') {
              e.preventDefault();
              onCatalogHealthClick?.(catalog.name);
            }
          }}
        >
          {catalog.name}
        </span>
        {catalog.connected ? (
          <CheckCircle className="w-4 h-4 text-green-500" />
        ) : (
          <XCircle className="w-4 h-4 text-red-500" />
        )}
        {onCatalogHealthClick && (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onCatalogHealthClick(catalog.name);
            }}
            className="p-1 hover:bg-iceberg/10 rounded transition-colors"
            title="Catalog Health Dashboard"
          >
            <Activity className="w-4 h-4 text-iceberg" />
          </button>
        )}
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
              <span className="text-sm">Loading...</span>
            </div>
          ) : Object.keys(tablesByNamespace).length === 0 ? (
            <div className="px-2 py-1.5 text-sm text-gray-500">No tables found</div>
          ) : (
            Object.entries(tablesByNamespace).map(([namespace, nsTables]) => (
              <NamespaceTreeItem
                key={namespace}
                catalogName={catalog.name}
                namespace={namespace}
                tables={nsTables}
                onTableSelect={onTableSelect}
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
  tables: TableInfo[];
  onTableSelect: (catalog: string, namespace: string, table: string) => void;
  selectedTable?: { catalog: string; namespace: string; table: string };
}

function NamespaceTreeItem({
  catalogName,
  namespace,
  tables,
  onTableSelect,
  selectedTable,
}: NamespaceTreeItemProps) {
  const [isExpanded, setIsExpanded] = useState(true);

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
        <span className="text-xs text-gray-400">({tables.length})</span>
      </div>

      {isExpanded && (
        <div className="ml-4">
          {tables.map((table) => {
            const isSelected =
              selectedTable?.catalog === catalogName &&
              selectedTable?.namespace === namespace &&
              selectedTable?.table === table.name;

            return (
              <div
                key={table.name}
                className={`flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer ${
                  isSelected
                    ? 'bg-iceberg/10 text-iceberg'
                    : 'hover:bg-gray-100 dark:hover:bg-gray-700'
                }`}
                onClick={() => onTableSelect(catalogName, namespace, table.name)}
              >
                <Table className="w-4 h-4" />
                <span className="text-sm truncate">{table.name}</span>
                <span className="text-xs text-gray-400 ml-auto">
                  {table.snapshot_count} snaps
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function Sidebar({ onTableSelect, onAddCatalog, onCatalogHealthClick, selectedTable }: SidebarProps) {
  const { data: catalogs, isLoading } = useCatalogs();
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
      </div>

      <div className="flex-1 overflow-y-auto p-2">
        {isLoading ? (
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
              onTableSelect={onTableSelect}
              onDelete={handleDelete}
              onCatalogHealthClick={onCatalogHealthClick}
              selectedTable={selectedTable}
            />
          ))
        )}
      </div>
    </aside>
  );
}
