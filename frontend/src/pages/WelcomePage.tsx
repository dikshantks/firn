import { Database } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

export function WelcomePage() {
  const navigate = useNavigate();

  return (
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
          type="button"
          onClick={() => navigate('/', { state: { openCatalogForm: true } })}
          className="px-6 py-3 bg-iceberg text-white rounded-lg hover:bg-iceberg-dark transition-colors font-medium"
        >
          Add Your First Catalog
        </button>
      </div>
    </div>
  );
}
