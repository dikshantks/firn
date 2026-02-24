import { ArrowRight, Plus, Minus, Equal, Loader2 } from 'lucide-react';
import { useSnapshotComparison } from '../../hooks/useIcebergData';
import type { SnapshotInfo } from '../../types/iceberg';

interface CompareSnapshotsProps {
  catalog: string;
  namespace: string;
  table: string;
  snapshot1: SnapshotInfo;
  snapshot2: SnapshotInfo;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(k));
  const sign = bytes < 0 ? '-' : '+';
  return `${sign}${(Math.abs(bytes) / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

export function CompareSnapshots({
  catalog,
  namespace,
  table,
  snapshot1,
  snapshot2,
}: CompareSnapshotsProps) {
  const { data: comparison, isLoading, error } = useSnapshotComparison(
    catalog,
    namespace,
    table,
    snapshot1.snapshot_id,
    snapshot2.snapshot_id
  );

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-8 text-red-500">
        <p>Error comparing snapshots</p>
        <p className="text-sm">{(error as Error).message}</p>
      </div>
    );
  }

  if (!comparison) {
    return null;
  }

  return (
    <div className="p-4 space-y-4">
      <div>
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-1">
          Snapshot Comparison
        </h3>
        <div className="flex items-center gap-2 text-sm text-gray-500">
          <span className="font-mono">#{snapshot1.snapshot_id.toString().slice(-6)}</span>
          <ArrowRight className="w-4 h-4" />
          <span className="font-mono">#{snapshot2.snapshot_id.toString().slice(-6)}</span>
        </div>
      </div>

      {/* File changes */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-3 text-center">
          <div className="flex items-center justify-center gap-1 text-green-600 dark:text-green-400 mb-1">
            <Plus className="w-4 h-4" />
            <span className="text-xs font-medium">Added</span>
          </div>
          <p className="text-2xl font-bold text-green-700 dark:text-green-300">
            {comparison.files_added}
          </p>
          <p className="text-xs text-green-600 dark:text-green-400">files</p>
        </div>
        <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-3 text-center">
          <div className="flex items-center justify-center gap-1 text-red-600 dark:text-red-400 mb-1">
            <Minus className="w-4 h-4" />
            <span className="text-xs font-medium">Removed</span>
          </div>
          <p className="text-2xl font-bold text-red-700 dark:text-red-300">
            {comparison.files_removed}
          </p>
          <p className="text-xs text-red-600 dark:text-red-400">files</p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3 text-center">
          <div className="flex items-center justify-center gap-1 text-gray-600 dark:text-gray-400 mb-1">
            <Equal className="w-4 h-4" />
            <span className="text-xs font-medium">Unchanged</span>
          </div>
          <p className="text-2xl font-bold text-gray-700 dark:text-gray-300">
            {comparison.files_unchanged}
          </p>
          <p className="text-xs text-gray-600 dark:text-gray-400">files</p>
        </div>
      </div>

      {/* Deltas */}
      {(comparison.records_delta !== null || comparison.size_delta_bytes !== null) && (
        <div className="grid grid-cols-2 gap-3">
          {comparison.records_delta !== null && (
            <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
              <p className="text-xs text-gray-500 mb-1">Record Change</p>
              <p className={`text-lg font-semibold ${
                comparison.records_delta > 0
                  ? 'text-green-600'
                  : comparison.records_delta < 0
                  ? 'text-red-600'
                  : 'text-gray-600'
              }`}>
                {comparison.records_delta > 0 ? '+' : ''}
                {comparison.records_delta.toLocaleString()}
              </p>
            </div>
          )}
          {comparison.size_delta_bytes !== null && (
            <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
              <p className="text-xs text-gray-500 mb-1">Size Change</p>
              <p className={`text-lg font-semibold ${
                comparison.size_delta_bytes > 0
                  ? 'text-green-600'
                  : comparison.size_delta_bytes < 0
                  ? 'text-red-600'
                  : 'text-gray-600'
              }`}>
                {formatBytes(comparison.size_delta_bytes)}
              </p>
            </div>
          )}
        </div>
      )}

      {/* Added files list */}
      {comparison.added_file_paths.length > 0 && (
        <div>
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            Added Files ({comparison.added_file_paths.length})
          </h4>
          <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-2 max-h-32 overflow-y-auto">
            {comparison.added_file_paths.slice(0, 10).map((path, idx) => (
              <div
                key={idx}
                className="text-xs font-mono text-green-700 dark:text-green-300 truncate py-0.5"
                title={path}
              >
                + {path.split('/').pop()}
              </div>
            ))}
            {comparison.added_file_paths.length > 10 && (
              <div className="text-xs text-green-600 dark:text-green-400 mt-1">
                ... and {comparison.added_file_paths.length - 10} more
              </div>
            )}
          </div>
        </div>
      )}

      {/* Removed files list */}
      {comparison.removed_file_paths.length > 0 && (
        <div>
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            Removed Files ({comparison.removed_file_paths.length})
          </h4>
          <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-2 max-h-32 overflow-y-auto">
            {comparison.removed_file_paths.slice(0, 10).map((path, idx) => (
              <div
                key={idx}
                className="text-xs font-mono text-red-700 dark:text-red-300 truncate py-0.5"
                title={path}
              >
                - {path.split('/').pop()}
              </div>
            ))}
            {comparison.removed_file_paths.length > 10 && (
              <div className="text-xs text-red-600 dark:text-red-400 mt-1">
                ... and {comparison.removed_file_paths.length - 10} more
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
