import {
  Clock,
  GitBranch,
  FileText,
  Database,
  Plus,
  Minus,
  Hash,
} from 'lucide-react';
import type { SnapshotInfo } from '../../types/iceberg';

interface SnapshotDetailProps {
  snapshot: SnapshotInfo;
}

export function SnapshotDetail({ snapshot }: SnapshotDetailProps) {
  const summary = snapshot.summary || {};
  
  const addedRecords = parseInt(summary['added-records'] as string) || 0;
  const deletedRecords = parseInt(summary['deleted-records'] as string) || 0;
  const addedFiles = parseInt(summary['added-data-files'] as string) || 0;
  const deletedFiles = parseInt(summary['deleted-data-files'] as string) || 0;
  const totalRecords = parseInt(summary['total-records'] as string) || 0;
  const totalFiles = parseInt(summary['total-data-files'] as string) || 0;

  return (
    <div className="p-4 space-y-4">
      <div>
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-1">
          Snapshot Details
        </h3>
        <p className="text-sm text-gray-500 font-mono">
          ID: {snapshot.snapshot_id}
        </p>
      </div>

      {/* Basic info */}
      <div className="grid grid-cols-2 gap-3">
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <GitBranch className="w-4 h-4" />
            <span className="text-xs">Operation</span>
          </div>
          <p className="text-sm font-semibold text-gray-900 dark:text-white capitalize">
            {snapshot.operation}
          </p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <Clock className="w-4 h-4" />
            <span className="text-xs">Timestamp</span>
          </div>
          <p className="text-sm font-semibold text-gray-900 dark:text-white">
            {new Date(snapshot.timestamp_ms).toLocaleString()}
          </p>
        </div>
      </div>

      {/* Parent snapshot */}
      {snapshot.parent_snapshot_id && (
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <Hash className="w-4 h-4" />
            <span className="text-xs">Parent Snapshot</span>
          </div>
          <p className="text-sm font-mono text-gray-900 dark:text-white">
            {snapshot.parent_snapshot_id}
          </p>
        </div>
      )}

      {/* Changes */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          Changes
        </h4>
        <div className="grid grid-cols-2 gap-2">
          {addedRecords > 0 && (
            <div className="flex items-center gap-2 p-2 bg-green-50 dark:bg-green-900/20 rounded text-green-700 dark:text-green-400">
              <Plus className="w-4 h-4" />
              <span className="text-sm">
                {addedRecords.toLocaleString()} records added
              </span>
            </div>
          )}
          {deletedRecords > 0 && (
            <div className="flex items-center gap-2 p-2 bg-red-50 dark:bg-red-900/20 rounded text-red-700 dark:text-red-400">
              <Minus className="w-4 h-4" />
              <span className="text-sm">
                {deletedRecords.toLocaleString()} records deleted
              </span>
            </div>
          )}
          {addedFiles > 0 && (
            <div className="flex items-center gap-2 p-2 bg-green-50 dark:bg-green-900/20 rounded text-green-700 dark:text-green-400">
              <FileText className="w-4 h-4" />
              <span className="text-sm">
                {addedFiles.toLocaleString()} files added
              </span>
            </div>
          )}
          {deletedFiles > 0 && (
            <div className="flex items-center gap-2 p-2 bg-red-50 dark:bg-red-900/20 rounded text-red-700 dark:text-red-400">
              <FileText className="w-4 h-4" />
              <span className="text-sm">
                {deletedFiles.toLocaleString()} files deleted
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Totals */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          Totals (after this snapshot)
        </h4>
        <div className="grid grid-cols-2 gap-3">
          <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
            <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
              <Database className="w-4 h-4" />
              <span className="text-xs">Total Records</span>
            </div>
            <p className="text-lg font-semibold text-gray-900 dark:text-white">
              {totalRecords.toLocaleString()}
            </p>
          </div>
          <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
            <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
              <FileText className="w-4 h-4" />
              <span className="text-xs">Total Files</span>
            </div>
            <p className="text-lg font-semibold text-gray-900 dark:text-white">
              {totalFiles.toLocaleString()}
            </p>
          </div>
        </div>
      </div>

      {/* Raw summary */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          Full Summary
        </h4>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3 max-h-48 overflow-y-auto">
          <pre className="text-xs font-mono text-gray-600 dark:text-gray-400 whitespace-pre-wrap">
            {JSON.stringify(summary, null, 2)}
          </pre>
        </div>
      </div>
    </div>
  );
}
