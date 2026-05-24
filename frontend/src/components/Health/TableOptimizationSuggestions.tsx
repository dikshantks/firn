import { AlertTriangle, CheckCircle2, Gauge, TerminalSquare } from 'lucide-react';
import { useTableHealth } from '../../hooks/useHealth';

interface TableOptimizationSuggestionsProps {
  catalog: string;
  namespace: string;
  table: string;
}

const priorityClasses: Record<string, string> = {
  high: 'bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300',
  medium: 'bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300',
  low: 'bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300',
};

export function TableOptimizationSuggestions({
  catalog,
  namespace,
  table,
}: TableOptimizationSuggestionsProps) {
  const { data: health, isLoading, error } = useTableHealth(catalog, namespace, table);

  if (isLoading) {
    return (
      <div className="p-6 text-sm text-gray-500">
        Loading optimization suggestions...
      </div>
    );
  }

  if (error || !health) {
    return null;
  }

  return (
    <div className="p-6 space-y-5">
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          <Gauge className="w-5 h-5 text-iceberg" />
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
            Optimization Suggestions
          </h3>
        </div>
        <span className="px-2 py-1 rounded text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200">
          Health score {health.health_score}/100
        </span>
        <span className="px-2 py-1 rounded text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 capitalize">
          {health.status}
        </span>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Metric label="Snapshots" value={health.metrics.total_snapshots} />
        <Metric label="Small files" value={health.metrics.small_files_count} />
        <Metric label="Delete files" value={health.metrics.total_delete_files} />
        <Metric label="Size GB" value={health.metrics.total_size_gb.toFixed(2)} />
      </div>

      {health.recommendations.length === 0 ? (
        <div className="flex items-center gap-3 p-4 rounded border border-green-200 dark:border-green-900 bg-green-50 dark:bg-green-950/40 text-green-800 dark:text-green-200">
          <CheckCircle2 className="w-5 h-5" />
          <p className="text-sm">No optimization recommendations for this table.</p>
        </div>
      ) : (
        <div className="space-y-3">
          {health.recommendations.map((recommendation, index) => (
            <div
              key={`${recommendation.type}-${index}`}
              className="p-4 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800"
            >
              <div className="flex flex-wrap items-center gap-2 mb-2">
                <AlertTriangle className="w-4 h-4 text-amber-500" />
                <h4 className="font-medium text-gray-900 dark:text-white">
                  {recommendation.type.replace(/_/g, ' ')}
                </h4>
                <span
                  className={`px-2 py-0.5 rounded text-xs font-medium ${
                    priorityClasses[recommendation.priority] || priorityClasses.low
                  }`}
                >
                  {recommendation.priority}
                </span>
              </div>
              <p className="text-sm text-gray-600 dark:text-gray-300 mb-2">
                {recommendation.reason}
              </p>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Impact: {recommendation.estimated_impact}
              </p>
              {recommendation.command_example && (
                <div className="mt-3 rounded bg-gray-950 text-gray-100 p-3 overflow-x-auto">
                  <div className="flex items-center gap-2 mb-2 text-xs text-gray-400">
                    <TerminalSquare className="w-4 h-4" />
                    Proposed command
                  </div>
                  <code className="text-xs whitespace-pre-wrap">
                    {recommendation.command_example}
                  </code>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 p-3">
      <p className="text-xs text-gray-500 dark:text-gray-400">{label}</p>
      <p className="text-lg font-semibold text-gray-900 dark:text-white">{value}</p>
    </div>
  );
}
