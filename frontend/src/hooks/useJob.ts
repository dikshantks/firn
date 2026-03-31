/**
 * React hooks for job progress tracking via SSE.
 */

import { useState, useEffect, useCallback } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { jobApi, JobStatus } from '../services/api';

// Query keys
export const jobKeys = {
  all: ['jobs'] as const,
  detail: (jobId: string) => ['jobs', jobId] as const,
};

/**
 * Hook to get a job's current status (polling-based).
 */
export function useJob(jobId: string | null) {
  return useQuery({
    queryKey: jobKeys.detail(jobId || ''),
    queryFn: () => jobApi.get(jobId!),
    enabled: !!jobId,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data?.status === 'completed' || data?.status === 'failed') {
        return false;
      }
      return 1000;
    },
  });
}

/**
 * Hook for real-time job progress tracking via Server-Sent Events.
 * 
 * This provides a more responsive experience than polling, with
 * immediate updates when the job status changes.
 * 
 * @param jobId - The job ID to track, or null to disable
 * @returns Job progress state and control functions
 */
export function useJobProgress(jobId: string | null) {
  const [progress, setProgress] = useState(0);
  const [status, setStatus] = useState<'pending' | 'running' | 'completed' | 'failed'>('pending');
  const [message, setMessage] = useState('');
  const [result, setResult] = useState<unknown>(null);
  const [error, setError] = useState<string | null>(null);
  const [isConnected, setIsConnected] = useState(false);

  const queryClient = useQueryClient();

  const reset = useCallback(() => {
    setProgress(0);
    setStatus('pending');
    setMessage('');
    setResult(null);
    setError(null);
    setIsConnected(false);
  }, []);

  useEffect(() => {
    if (!jobId) {
      reset();
      return;
    }

    const eventSource = new EventSource(jobApi.streamUrl(jobId));
    setIsConnected(true);

    eventSource.onopen = () => {
      setIsConnected(true);
    };

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        
        if (data.error && !data.status) {
          setError(data.error);
          eventSource.close();
          setIsConnected(false);
          return;
        }

        setProgress(data.progress || 0);
        setStatus(data.status || 'pending');
        setMessage(data.message || '');

        if (data.result !== undefined) {
          setResult(data.result);
        }

        if (data.error) {
          setError(data.error);
        }

        if (data.status === 'completed' || data.status === 'failed') {
          eventSource.close();
          setIsConnected(false);
          queryClient.invalidateQueries({ queryKey: ['catalogs'] });
        }
      } catch (e) {
        console.error('Error parsing SSE data:', e);
      }
    };

    eventSource.onerror = () => {
      setIsConnected(false);
      eventSource.close();
    };

    return () => {
      eventSource.close();
      setIsConnected(false);
    };
  }, [jobId, queryClient, reset]);

  return {
    progress,
    status,
    message,
    result,
    error,
    isConnected,
    isLoading: status === 'pending' || status === 'running',
    isSuccess: status === 'completed',
    isError: status === 'failed',
    reset,
  };
}

/**
 * Hook to list all jobs.
 */
export function useJobs(limit = 100) {
  return useQuery({
    queryKey: jobKeys.all,
    queryFn: () => jobApi.list(limit),
  });
}
