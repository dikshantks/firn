import { Database, CheckCircle, XCircle, Trash2, RefreshCw } from 'lucide-react';
import { useCatalogs, useDeleteCatalog, useCatalogTest } from '../../hooks/useCatalog';
import type { CatalogInfo } from '../../types/iceberg';

interface CatalogCardProps {
  catalog: CatalogInfo;
  onDelete: (name: string) => void;
  onTest: (name: string) => void;
}

function CatalogCard({ catalog, onDelete, onTest }: CatalogCardProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
      <div className="flex items-start justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-iceberg/10 rounded-lg">
            <Database className="w-6 h-6 text-iceberg" />
          </div>
          <div>
            <h3 className="font-semibold text-gray-900 dark:text-white">
              {catalog.name}
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400 capitalize">
              {catalog.type}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-1">
          {catalog.connected ? (
            <CheckCircle className="w-5 h-5 text-green-500" />
          ) : (
            <XCircle className="w-5 h-5 text-red-500" />
          )}
        </div>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
        <div>
          <span className="text-gray-500 dark:text-gray-400">Namespaces</span>
          <p className="font-medium text-gray-900 dark:text-white">
            {catalog.namespace_count ?? '-'}
          </p>
        </div>
        <div>
          <span className="text-gray-500 dark:text-gray-400">Tables</span>
          <p className="font-medium text-gray-900 dark:text-white">
            {catalog.table_count ?? '-'}
          </p>
        </div>
      </div>

      <div className="mt-4 flex justify-end gap-2">
        <button
          onClick={() => onTest(catalog.name)}
          className="p-2 text-gray-500 hover:text-iceberg hover:bg-iceberg/10 rounded-lg transition-colors"
          title="Test connection"
        >
          <RefreshCw className="w-4 h-4" />
        </button>
        <button
          onClick={() => onDelete(catalog.name)}
          className="p-2 text-gray-500 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-900/30 rounded-lg transition-colors"
          title="Remove catalog"
        >
          <Trash2 className="w-4 h-4" />
        </button>
      </div>
    </div>
  );
}

export function CatalogList() {
  const { data: catalogs, isLoading } = useCatalogs();
  const deleteCatalog = useDeleteCatalog();

  const handleDelete = (name: string) => {
    if (confirm(`Are you sure you want to remove catalog "${name}"?`)) {
      deleteCatalog.mutate(name);
    }
  };

  const handleTest = (name: string) => {
    // This would trigger a test - for now just log
    console.log('Testing catalog:', name);
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-iceberg"></div>
      </div>
    );
  }

  if (!catalogs || catalogs.length === 0) {
    return (
      <div className="text-center py-12">
        <Database className="w-16 h-16 mx-auto text-gray-300 dark:text-gray-600 mb-4" />
        <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
          No catalogs configured
        </h3>
        <p className="text-gray-500 dark:text-gray-400">
          Add a catalog to start exploring Iceberg tables
        </p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {catalogs.map((catalog) => (
        <CatalogCard
          key={catalog.name}
          catalog={catalog}
          onDelete={handleDelete}
          onTest={handleTest}
        />
      ))}
    </div>
  );
}
