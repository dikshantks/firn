import { Loader2, HardDrive, File, Database, Layers } from 'lucide-react';
import {
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { useStorageAnalytics } from '../../hooks/useIcebergData';

interface StorageStatsProps {
  catalog: string;
  namespace: string;
  table: string;
}

const COLORS = ['#0ea5e9', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6'];

export function StorageStats({ catalog, namespace, table }: StorageStatsProps) {
  const { data: analytics, isLoading, error } = useStorageAnalytics(
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
        <p>Error loading storage analytics</p>
        <p className="text-sm">{(error as Error).message}</p>
      </div>
    );
  }

  if (!analytics) {
    return null;
  }

  // Prepare chart data
  const formatData = Object.entries(analytics.format_breakdown).map(([name, value]) => ({
    name,
    value,
  }));

  const sizeData = Object.entries(analytics.size_distribution).map(([name, value]) => ({
    name,
    value,
  }));

  return (
    <div className="p-4 space-y-6">
      <div>
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
          Storage Analytics
        </h3>
        <p className="text-sm text-gray-500">
          Current snapshot storage breakdown
        </p>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-2">
            <HardDrive className="w-5 h-5" />
            <span className="text-sm">Total Size</span>
          </div>
          <p className="text-2xl font-bold text-gray-900 dark:text-white">
            {analytics.total_size_human}
          </p>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-2">
            <File className="w-5 h-5" />
            <span className="text-sm">Total Files</span>
          </div>
          <p className="text-2xl font-bold text-gray-900 dark:text-white">
            {analytics.total_files.toLocaleString()}
          </p>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-2">
            <Database className="w-5 h-5" />
            <span className="text-sm">Total Records</span>
          </div>
          <p className="text-2xl font-bold text-gray-900 dark:text-white">
            {analytics.total_records.toLocaleString()}
          </p>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-2">
            <Layers className="w-5 h-5" />
            <span className="text-sm">Avg File Size</span>
          </div>
          <p className="text-2xl font-bold text-gray-900 dark:text-white">
            {analytics.avg_file_size_human}
          </p>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Format breakdown */}
        {formatData.length > 0 && (
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
            <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-4">
              File Format Distribution
            </h4>
            <div className="h-48">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={formatData}
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={70}
                    paddingAngle={2}
                    dataKey="value"
                    label={({ name, percent }) =>
                      `${name} (${(percent * 100).toFixed(0)}%)`
                    }
                  >
                    {formatData.map((_, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={COLORS[index % COLORS.length]}
                      />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}

        {/* Size distribution */}
        {sizeData.length > 0 && (
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
            <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-4">
              File Size Distribution
            </h4>
            <div className="h-48">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={sizeData}>
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 10 }}
                    interval={0}
                    angle={-45}
                    textAnchor="end"
                    height={60}
                  />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Bar dataKey="value" fill="#0ea5e9" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}
      </div>

      {/* Partition breakdown */}
      {analytics.partition_count > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-4">
            Partition Breakdown ({analytics.partition_count} partitions)
          </h4>
          <div className="max-h-64 overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-white dark:bg-gray-800">
                <tr className="text-left text-gray-500">
                  <th className="pb-2">Partition</th>
                  <th className="pb-2 text-right">Files</th>
                  <th className="pb-2 text-right">Records</th>
                  <th className="pb-2 text-right">Size</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(analytics.partition_breakdown)
                  .slice(0, 20)
                  .map(([partition, stats], idx) => (
                    <tr
                      key={idx}
                      className="border-t border-gray-100 dark:border-gray-700"
                    >
                      <td className="py-2 font-mono text-xs truncate max-w-[200px]">
                        {partition}
                      </td>
                      <td className="py-2 text-right">{stats.files}</td>
                      <td className="py-2 text-right">
                        {stats.records.toLocaleString()}
                      </td>
                      <td className="py-2 text-right">
                        {(stats.size_bytes / 1024 / 1024).toFixed(1)} MB
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
            {Object.keys(analytics.partition_breakdown).length > 20 && (
              <p className="text-xs text-gray-400 mt-2">
                Showing 20 of {Object.keys(analytics.partition_breakdown).length} partitions
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
