import { useState } from 'react';
import { X, Database, Cloud, Server } from 'lucide-react';
import { useCreateCatalog } from '../../hooks/useCatalog';
import type { CatalogType } from '../../types/iceberg';

interface CatalogFormProps {
  onClose: () => void;
}

export function CatalogForm({ onClose }: CatalogFormProps) {
  const [name, setName] = useState('');
  const [type, setType] = useState<CatalogType>('hive');
  const [properties, setProperties] = useState<Record<string, string>>({
    // Hive
    uri: '',
    's3.endpoint': '',
    's3.access-key-id': '',
    's3.secret-access-key': '',
    // Glue
    'glue.access-key-id': '',
    'glue.secret-access-key': '',
    'glue.session-token': '',
    'glue.region': '',
    'glue.profile-name': '',
  });

  const createCatalog = useCreateCatalog();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    // Filter out empty properties
    const filteredProps = Object.fromEntries(
      Object.entries(properties).filter(([_, v]) => v.trim() !== '')
    );

    try {
      await createCatalog.mutateAsync({
        name,
        type,
        properties: filteredProps,
      });
      onClose();
    } catch (error) {
      console.error('Failed to create catalog:', error);
    }
  };

  const handlePropertyChange = (key: string, value: string) => {
    setProperties((prev) => ({ ...prev, [key]: value }));
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-md mx-4">
        <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
            Add Catalog
          </h2>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-100 dark:hover:bg-gray-700 rounded"
          >
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-4 space-y-4">
          {/* Catalog Name */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Catalog Name
            </label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-catalog"
              required
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
            />
          </div>

          {/* Catalog Type */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Catalog Type
            </label>
            <div className="grid grid-cols-2 gap-2">
              <button
                type="button"
                onClick={() => setType('hive')}
                className={`flex items-center gap-2 p-3 border rounded-lg transition-colors ${
                  type === 'hive'
                    ? 'border-iceberg bg-iceberg/10 text-iceberg'
                    : 'border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
                }`}
              >
                <Server className="w-5 h-5" />
                <span className="font-medium">Hive</span>
              </button>
              <button
                type="button"
                onClick={() => setType('glue')}
                className={`flex items-center gap-2 p-3 border rounded-lg transition-colors ${
                  type === 'glue'
                    ? 'border-iceberg bg-iceberg/10 text-iceberg'
                    : 'border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
                }`}
              >
                <Cloud className="w-5 h-5" />
                <span className="font-medium">AWS Glue</span>
              </button>
            </div>
          </div>

          {/* Hive Properties */}
          {type === 'hive' && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Metastore URI
                </label>
                <input
                  type="text"
                  value={properties.uri}
                  onChange={(e) => handlePropertyChange('uri', e.target.value)}
                  placeholder="thrift://localhost:9083"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  S3 Endpoint (optional)
                </label>
                <input
                  type="text"
                  value={properties['s3.endpoint']}
                  onChange={(e) => handlePropertyChange('s3.endpoint', e.target.value)}
                  placeholder="http://localhost:9000"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Access Key
                  </label>
                  <input
                    type="text"
                    value={properties['s3.access-key-id']}
                    onChange={(e) =>
                      handlePropertyChange('s3.access-key-id', e.target.value)
                    }
                    placeholder="minioadmin"
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Secret Key
                  </label>
                  <input
                    type="password"
                    value={properties['s3.secret-access-key']}
                    onChange={(e) =>
                      handlePropertyChange('s3.secret-access-key', e.target.value)
                    }
                    placeholder="********"
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  />
                </div>
              </div>
            </>
          )}

          {/* Glue Properties */}
          {type === 'glue' && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  AWS Region
                </label>
                <input
                  type="text"
                  value={properties['glue.region'] || ''}
                  onChange={(e) => handlePropertyChange('glue.region', e.target.value)}
                  placeholder="ap-south-1"
                  required
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  S3 Endpoint (optional, for MinIO)
                </label>
                <input
                  type="text"
                  value={properties['s3.endpoint'] || ''}
                  onChange={(e) => handlePropertyChange('s3.endpoint', e.target.value)}
                  placeholder="http://localhost:9000"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Access Key
                  </label>
                  <input
                    type="text"
                    value={properties['glue.access-key-id'] || ''}
                    onChange={(e) =>
                      handlePropertyChange('glue.access-key-id', e.target.value)
                    }
                    placeholder="minioadmin"
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Secret Key
                  </label>
                  <input
                    type="password"
                    value={properties['glue.secret-access-key'] || ''}
                    onChange={(e) =>
                      handlePropertyChange('glue.secret-access-key', e.target.value)
                    }
                    placeholder="********"
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                  />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Session Token (optional)
                </label>
                <input
                  type="password"
                  value={properties['glue.session-token'] || ''}
                  onChange={(e) =>
                    handlePropertyChange('glue.session-token', e.target.value)
                  }
                  placeholder="For temporary AWS credentials"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  AWS Profile (optional)
                </label>
                <input
                  type="text"
                  value={properties['glue.profile-name'] || ''}
                  onChange={(e) =>
                    handlePropertyChange('glue.profile-name', e.target.value)
                  }
                  placeholder="default"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-iceberg focus:border-transparent dark:bg-gray-700 dark:text-white"
                />
              </div>
            </>
          )}

          {/* Error message */}
          {createCatalog.isError && (
            <div className="p-3 bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 rounded-lg text-red-700 dark:text-red-300 text-sm">
              {(createCatalog.error as Error)?.message || 'Failed to create catalog'}
            </div>
          )}

          {/* Submit button */}
          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={createCatalog.isPending || !name}
              className="px-4 py-2 bg-iceberg text-white rounded-lg hover:bg-iceberg-dark transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
            >
              {createCatalog.isPending ? (
                <>
                  <span className="animate-spin">⏳</span>
                  Connecting...
                </>
              ) : (
                <>
                  <Database className="w-4 h-4" />
                  Add Catalog
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
