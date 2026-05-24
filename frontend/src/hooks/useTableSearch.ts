import { useMemo } from 'react';
import { useQueries } from '@tanstack/react-query';
import { healthApi } from '../services/api';
import { tableApi } from '../services/api';
import { healthKeys } from './useHealth';
import { namespaceKeys, tableKeys } from './useIcebergData';
import type { CatalogInfo } from '../types/iceberg';

export interface TableSearchResult {
  catalog: string;
  namespace: string;
  table: string;
}

const MAX_GLUE_SEARCH_NAMESPACES = 8;

export function useTableSearch(catalogs: CatalogInfo[] | undefined, query: string) {
  const trimmedQuery = query.trim();
  const shouldSearch = trimmedQuery.length >= 2;

  const cacheInfoQueries = useQueries({
    queries: (catalogs || []).map((catalog) => ({
      queryKey: healthKeys.cacheInfo(catalog.name),
      queryFn: () => healthApi.getCacheInfo(catalog.name),
      enabled: shouldSearch && catalog.connected,
      staleTime: 30 * 1000,
    })),
  });

  const cachedSearchQueries = useQueries({
    queries: (catalogs || []).map((catalog, index) => ({
      queryKey: ['health', 'search', catalog.name, trimmedQuery] as const,
      queryFn: () => healthApi.searchCachedTables(catalog.name, trimmedQuery),
      enabled:
        shouldSearch &&
        catalog.connected &&
        (cacheInfoQueries[index]?.data?.cached_tables ?? 0) > 0,
      staleTime: 10 * 1000,
    })),
  });

  const catalogsWithoutCache = (catalogs || []).filter(
    (catalog, index) =>
      catalog.connected && (cacheInfoQueries[index]?.data?.cached_tables ?? 0) === 0
  );

  const namespaceQueries = useQueries({
    queries: catalogsWithoutCache.map((catalog) => ({
      queryKey: namespaceKeys.all(catalog.name),
      queryFn: () => tableApi.listNamespaces(catalog.name),
      enabled: shouldSearch,
      staleTime: 5 * 60 * 1000,
    })),
  });

  const namespaceTargets = catalogsWithoutCache.flatMap((catalog, catalogIndex) => {
    const namespaces = namespaceQueries[catalogIndex]?.data || [];
    return namespaces.slice(0, MAX_GLUE_SEARCH_NAMESPACES).map((namespace) => ({
      catalog: catalog.name,
      namespace,
    }));
  });

  const tableQueries = useQueries({
    queries: namespaceTargets.map(({ catalog, namespace }) => ({
      queryKey: tableKeys.byNamespace(catalog, namespace, true),
      queryFn: () => tableApi.list(catalog, { namespace, lazy: true }),
      enabled: shouldSearch,
      staleTime: 5 * 60 * 1000,
    })),
  });

  const cachedResults = useMemo(
    () =>
      cachedSearchQueries.flatMap((searchQuery, index) => {
        const catalog = catalogs?.[index];
        if (!catalog || !searchQuery.data) {
          return [];
        }
        return searchQuery.data.map((row) => ({
          catalog: row.catalog,
          namespace: row.namespace,
          table: row.table_name,
        }));
      }),
    [cachedSearchQueries, catalogs]
  );

  const glueResults = namespaceTargets.flatMap(({ catalog, namespace }, index) => {
    const tables = tableQueries[index]?.data || [];
    const loweredQuery = trimmedQuery.toLowerCase();
    return tables
      .map((table) => ({
        catalog,
        namespace,
        table: table.name,
      }))
      .filter((result) => {
        const tableName = result.table.toLowerCase();
        const qualifiedName = `${result.namespace}.${result.table}`.toLowerCase();
        return tableName.includes(loweredQuery) || qualifiedName.includes(loweredQuery);
      });
  });

  const mergedResults = [...cachedResults, ...glueResults];
  const uniqueResults = mergedResults.filter(
    (result, index, all) =>
      all.findIndex(
        (item) =>
          item.catalog === result.catalog &&
          item.namespace === result.namespace &&
          item.table === result.table
      ) === index
  );

  return {
    results: uniqueResults.slice(0, 50),
    isSearching:
      shouldSearch &&
      (cacheInfoQueries.some((item) => item.isFetching) ||
        cachedSearchQueries.some((item) => item.isFetching) ||
        namespaceQueries.some((item) => item.isFetching) ||
        tableQueries.some((item) => item.isFetching)),
    shouldSearch,
    usesCachedSearch: cachedResults.length > 0,
  };
}
