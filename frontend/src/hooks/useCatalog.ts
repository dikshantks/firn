/**
 * React Query hooks for catalog operations.
 */

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { catalogApi } from '../services/api';
import type { CatalogCreate, CatalogInfo } from '../types/iceberg';

// Query keys
export const catalogKeys = {
  all: ['catalogs'] as const,
  detail: (name: string) => ['catalogs', name] as const,
  test: (name: string) => ['catalogs', name, 'test'] as const,
};

/**
 * Hook to list all catalogs.
 */
export function useCatalogs() {
  return useQuery({
    queryKey: catalogKeys.all,
    queryFn: catalogApi.list,
  });
}

/**
 * Hook to get a specific catalog.
 */
export function useCatalog(name: string) {
  return useQuery({
    queryKey: catalogKeys.detail(name),
    queryFn: () => catalogApi.get(name),
    enabled: !!name,
  });
}

/**
 * Hook to test catalog connectivity.
 */
export function useCatalogTest(name: string, enabled = false) {
  return useQuery({
    queryKey: catalogKeys.test(name),
    queryFn: () => catalogApi.test(name),
    enabled: enabled && !!name,
  });
}

/**
 * Hook to create a new catalog.
 */
export function useCreateCatalog() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (data: CatalogCreate) => catalogApi.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: catalogKeys.all });
    },
  });
}

/**
 * Hook to delete a catalog.
 */
export function useDeleteCatalog() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (name: string) => catalogApi.delete(name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: catalogKeys.all });
    },
  });
}
