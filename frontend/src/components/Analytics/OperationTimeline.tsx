import { Loader2, GitBranch, Plus, Minus, RefreshCw } from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import { useOperationHistory } from '../../hooks/useIcebergData';

interface OperationTimelineProps {
  catalog: string;
  namespace: string;
  table: string;
}

export function OperationTimeline({ catalog, namespace, table }: OperationTimelineProps) {
  const { data: history, isLoading, error } = useOperationHistory(
    catalog,
    namespace,
    table
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
        <p>Error loading operation history</p>
        <p className="text-sm">{(error as Error).message}</p>
      </div>
    );
  }

  if (!history || history.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500">
        <GitBranch className="w-12 h-12 mx-auto mb-4 opacity-50" />
        <p>No operation history available</p>
      </div>
    );
  }

  // Prepare chart data (reverse to show oldest first)
  const chartData = [...history].reverse().map((entry) => ({
    timestamp: new Date(entry.timestamp_ms).toLocaleDateString(),
    records: entry.total_records || 0,
    files: entry.total_data_files || 0,
    added: entry.added_records,
    deleted: entry.deleted_records,
  }));

  const getOperationIcon = (operation: string) => {
    switch (operation.toLowerCase()) {
      case 'append':
        return <Plus className="w-4 h-4 text-green-500" />;
      case 'delete':
        return <Minus className="w-4 h-4 text-red-500" />;
      case 'overwrite':
      case 'replace':
        return <RefreshCw className="w-4 h-4 text-orange-500" />;
      default:
        return <GitBranch className="w-4 h-4 text-gray-500" />;
    }
  };

  const getOperationColor = (operation: string) => {
    switch (operation.toLowerCase()) {
      case 'append':
        return 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400';
      case 'delete':
        return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400';
      case 'overwrite':
      case 'replace':
        return 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400';
      default:
        return 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400';
    }
  };

  return (
    <div className="p-4 space-y-6">
      <div>
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
          Operation Timeline
        </h3>
        <p className="text-sm text-gray-500">
          {history.length} operations recorded
        </p>
      </div>

      {/* Records over time chart */}
      {chartData.length > 1 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-4">
            Records Over Time
          </h4>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                <XAxis
                  dataKey="timestamp"
                  tick={{ fontSize: 10 }}
                  interval="preserveStartEnd"
                />
                <YAxis tick={{ fontSize: 10 }} />
                <Tooltip />
                <Line
                  type="monotone"
                  dataKey="records"
                  stroke="#0ea5e9"
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  name="Total Records"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Operation list */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Recent Operations
          </h4>
        </div>
        <div className="max-h-96 overflow-y-auto">
          {history.map((entry, idx) => (
            <div
              key={idx}
              className="px-4 py-3 border-b border-gray-100 dark:border-gray-700 last:border-b-0 hover:bg-gray-50 dark:hover:bg-gray-700/50"
            >
              <div className="flex items-start justify-between">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-gray-100 dark:bg-gray-700 rounded-lg">
                    {getOperationIcon(entry.operation)}
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <span
                        className={`px-2 py-0.5 rounded text-xs font-medium capitalize ${getOperationColor(
                          entry.operation
                        )}`}
                      >
                        {entry.operation}
                      </span>
                      <span className="text-xs text-gray-500 font-mono">
                        #{entry.snapshot_id.toString().slice(-6)}
                      </span>
                    </div>
                    <p className="text-xs text-gray-500 mt-1">
                      {new Date(entry.timestamp_ms).toLocaleString()}
                    </p>
                  </div>
                </div>
                <div className="text-right text-sm">
                  {entry.added_records > 0 && (
                    <p className="text-green-600">
                      +{entry.added_records.toLocaleString()} records
                    </p>
                  )}
                  {entry.deleted_records > 0 && (
                    <p className="text-red-600">
                      -{entry.deleted_records.toLocaleString()} records
                    </p>
                  )}
                  {entry.added_data_files > 0 && (
                    <p className="text-gray-500 text-xs">
                      +{entry.added_data_files} files
                    </p>
                  )}
                  {entry.deleted_data_files > 0 && (
                    <p className="text-gray-500 text-xs">
                      -{entry.deleted_data_files} files
                    </p>
                  )}
                </div>
              </div>
              {entry.total_records !== null && (
                <div className="mt-2 text-xs text-gray-400">
                  Total after: {entry.total_records?.toLocaleString()} records,{' '}
                  {entry.total_data_files} files
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
