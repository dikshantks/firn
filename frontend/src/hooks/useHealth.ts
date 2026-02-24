import { useQuery } from '@tanstack/react-query';
import { healthApi } from '../services/api';

export function useHealthSummary(catalog: string) {
  return useQuery({
    queryKey: ['health', 'summary', catalog],
    queryFn: () => healthApi.getSummary(catalog),
    enabled: !!catalog,
  });
}
