import { Database, CheckCircle, XCircle, Loader2, ArrowLeft } from 'lucide-react';
import { useHealthSummary } from '../../hooks/useHealth';

interface CatalogHealthDashboardProps {
  catalogName: string;
  onBack?: () => void;
}

export function CatalogHealthDashboard({ catalogName, onBack }: CatalogHealthDashboardProps) {
  const { data: summary, isLoading, error } = useHealthSummary(catalogName);

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center h-full">
        <Loader2 className="w-12 h-12 animate-spin text-iceberg mb-4" />
        <p className="text-gray-500">Scanning catalog health...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-red-500">
        <XCircle className="w-12 h-12 mb-4" />
        <p>Failed to load health data</p>
      </div>
    );
  }

  if (!summary) return null;

  const okCount = summary.healthy_tables;
  const notOkCount = summary.warning_tables + summary.critical_tables;

  return (
    <div className="p-6 max-w-2xl mx-auto">
      {onBack && (
        <button
          type="button"
          onClick={onBack}
          className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 mb-4"
        >
          <ArrowLeft className="w-4 h-4" />
          Back
        </button>
      )}
      <div className="flex items-center gap-3 mb-6">
        <Database className="w-10 h-10 text-iceberg" />
        <div>
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
            Catalog Health Dashboard
          </h2>
          <p className="text-sm text-gray-500">{catalogName}</p>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4 border border-green-200 dark:border-green-800">
          <div className="flex items-center gap-2 mb-2">
            <CheckCircle className="w-5 h-5 text-green-600 dark:text-green-400" />
            <span className="font-medium text-green-800 dark:text-green-200">OK</span>
          </div>
          <p className="text-2xl font-bold text-green-700 dark:text-green-300">
            {okCount}
          </p>
          <p className="text-sm text-green-600 dark:text-green-400">Healthy tables</p>
        </div>

        <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-4 border border-red-200 dark:border-red-800">
          <div className="flex items-center gap-2 mb-2">
            <XCircle className="w-5 h-5 text-red-600 dark:text-red-400" />
            <span className="font-medium text-red-800 dark:text-red-200">Not OK</span>
          </div>
          <p className="text-2xl font-bold text-red-700 dark:text-red-300">
            {notOkCount}
          </p>
          <p className="text-sm text-red-600 dark:text-red-400">
            Warning ({summary.warning_tables}) + Critical ({summary.critical_tables})
          </p>
        </div>
      </div>

      <p className="mt-4 text-sm text-gray-500">
        Total: {summary.total_tables} tables
      </p>
    </div>
  );
}
