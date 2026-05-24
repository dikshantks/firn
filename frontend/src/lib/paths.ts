export type ViewTab =
  | 'snapshots'
  | 'manifests'
  | 'files'
  | 'statistics'
  | 'storage'
  | 'timeline'
  | 'optimize';

export type AppViewTab =
  | 'snapshots'
  | 'manifests'
  | 'files'
  | 'statistics'
  | 'analytics'
  | 'timeline'
  | 'optimization';

const TAB_TO_PATH: Record<AppViewTab, ViewTab> = {
  snapshots: 'snapshots',
  manifests: 'manifests',
  files: 'files',
  statistics: 'statistics',
  analytics: 'storage',
  timeline: 'timeline',
  optimization: 'optimize',
};

const PATH_TO_TAB: Record<ViewTab, AppViewTab> = {
  snapshots: 'snapshots',
  manifests: 'manifests',
  files: 'files',
  statistics: 'statistics',
  storage: 'analytics',
  timeline: 'timeline',
  optimize: 'optimization',
};

export function encodeSegment(value: string): string {
  return encodeURIComponent(value);
}

export function decodeSegment(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  return decodeURIComponent(value);
}

export function homePath(): string {
  return '/';
}

export function catalogHealthPath(catalog: string): string {
  return `/catalogs/${encodeSegment(catalog)}/health`;
}

export function catalogHealthTablesPath(
  catalog: string,
  filter?: { type: string; value?: number }
): string {
  const params = new URLSearchParams();
  if (filter?.type) {
    params.set('type', filter.type);
  }
  if (filter?.value !== undefined) {
    params.set('value', String(filter.value));
  }
  const query = params.toString();
  return query
    ? `/catalogs/${encodeSegment(catalog)}/health/tables?${query}`
    : `/catalogs/${encodeSegment(catalog)}/health/tables`;
}

export function tablePath(
  catalog: string,
  namespace: string,
  table: string,
  tab: ViewTab = 'snapshots'
): string {
  const base = `/catalogs/${encodeSegment(catalog)}/${encodeSegment(namespace)}/${encodeSegment(table)}`;
  return tab === 'snapshots' ? base : `${base}/${tab}`;
}

export function tabPathFromAppTab(
  catalog: string,
  namespace: string,
  table: string,
  tab: AppViewTab
): string {
  return tablePath(catalog, namespace, table, TAB_TO_PATH[tab]);
}

export function appTabFromPathTab(tab: string | undefined): AppViewTab {
  if (!tab) {
    return 'snapshots';
  }
  if (tab in PATH_TO_TAB) {
    return PATH_TO_TAB[tab as ViewTab];
  }
  return 'snapshots';
}

export function isViewTab(value: string): value is ViewTab {
  return value in PATH_TO_TAB;
}
