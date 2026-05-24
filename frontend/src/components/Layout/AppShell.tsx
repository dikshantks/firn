import { ReactNode } from 'react';
import { useParams } from 'react-router-dom';
import { Header } from './Header';
import { Sidebar } from './Sidebar';
import { decodeSegment } from '../../lib/paths';

interface AppShellProps {
  children: ReactNode;
  onAddCatalog: () => void;
  onRefresh?: () => void;
}

export function AppShell({ children, onAddCatalog, onRefresh }: AppShellProps) {
  const { catalog, namespace, table } = useParams();
  const selectedTable =
    catalog && namespace && table
      ? {
          catalog: decodeSegment(catalog) ?? catalog,
          namespace: decodeSegment(namespace) ?? namespace,
          table: decodeSegment(table) ?? table,
        }
      : undefined;

  return (
    <div className="h-screen flex flex-col bg-gray-50 dark:bg-gray-900">
      <Header onRefresh={onRefresh} />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar onAddCatalog={onAddCatalog} selectedTable={selectedTable} />
        <main className="flex-1 overflow-auto">{children}</main>
      </div>
    </div>
  );
}
