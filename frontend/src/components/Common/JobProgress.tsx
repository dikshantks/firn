/**
 * Job progress display component with real-time updates.
 */

import { useEffect, useRef } from 'react';
import { useJobProgress } from '../../hooks/useJob';

interface JobProgressProps {
  jobId: string | null;
  onComplete?: (result: unknown) => void;
  onError?: (error: string) => void;
  showDetails?: boolean;
}

/**
 * Displays real-time progress for a background job.
 * 
 * Uses Server-Sent Events for instant updates without polling.
 */
export function JobProgress({ 
  jobId, 
  onComplete, 
  onError,
  showDetails = true,
}: JobProgressProps) {
  const { 
    progress, 
    status, 
    message, 
    result, 
    error, 
    isConnected,
    isSuccess,
    isError,
  } = useJobProgress(jobId);

  const completedRef = useRef(false);
  const erroredRef = useRef(false);

  useEffect(() => {
    if (isSuccess && onComplete && !completedRef.current) {
      completedRef.current = true;
      onComplete(result);
    }
  }, [isSuccess, onComplete, result]);

  useEffect(() => {
    if (isError && onError && error && !erroredRef.current) {
      erroredRef.current = true;
      onError(error);
    }
  }, [isError, onError, error]);

  if (!jobId) {
    return null;
  }

  const getStatusColor = () => {
    switch (status) {
      case 'completed':
        return 'bg-green-500';
      case 'failed':
        return 'bg-red-500';
      case 'running':
        return 'bg-iceberg';
      default:
        return 'bg-gray-400';
    }
  };

  const getStatusText = () => {
    switch (status) {
      case 'pending':
        return 'Waiting...';
      case 'running':
        return 'Processing...';
      case 'completed':
        return 'Completed';
      case 'failed':
        return 'Failed';
      default:
        return status;
    }
  };

  return (
    <div className="space-y-3 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${getStatusColor()} ${status === 'running' ? 'animate-pulse' : ''}`} />
          <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
            {getStatusText()}
          </span>
          {!isConnected && status === 'running' && (
            <span className="text-xs text-yellow-600 dark:text-yellow-400">
              (reconnecting...)
            </span>
          )}
        </div>
        <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">
          {progress}%
        </span>
      </div>

      <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2.5 overflow-hidden">
        <div
          className={`h-2.5 rounded-full transition-all duration-300 ease-out ${getStatusColor()}`}
          style={{ width: `${progress}%` }}
        />
      </div>

      {showDetails && message && (
        <p className="text-sm text-gray-600 dark:text-gray-400">
          {message}
        </p>
      )}

      {error && (
        <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
          <p className="text-sm text-red-700 dark:text-red-400">
            <span className="font-medium">Error:</span> {error}
          </p>
        </div>
      )}

      {isSuccess && showDetails && (
        <div className="p-3 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md">
          <p className="text-sm text-green-700 dark:text-green-400 font-medium">
            Operation completed successfully
          </p>
        </div>
      )}
    </div>
  );
}

/**
 * Compact inline progress indicator.
 */
export function JobProgressInline({ jobId }: { jobId: string | null }) {
  const { progress, status, message } = useJobProgress(jobId);

  if (!jobId) {
    return null;
  }

  return (
    <div className="flex items-center gap-3">
      <div className="flex-1 min-w-0">
        <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5">
          <div
            className={`h-1.5 rounded-full transition-all duration-300 ${
              status === 'failed' ? 'bg-red-500' : 
              status === 'completed' ? 'bg-green-500' : 'bg-iceberg'
            }`}
            style={{ width: `${progress}%` }}
          />
        </div>
      </div>
      <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">
        {message || `${progress}%`}
      </span>
    </div>
  );
}

export default JobProgress;
