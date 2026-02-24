import { ReactNode } from 'react';
import { Header } from './Header';
import { Sidebar } from './Sidebar';

interface AppShellProps {
  children: ReactNode;
  onTableSelect: (catalog: string, namespace: string, table: string) => void;
  onAddCatalog: () => void;
  onCatalogHealthClick?: (catalog: string) => void;
  onRefresh?: () => void;
  selectedTable?: { catalog: string; namespace: string; table: string };
}

export function AppShell({
  children,
  onTableSelect,
  onAddCatalog,
  onCatalogHealthClick,
  onRefresh,
  selectedTable,
}: AppShellProps) {
  return (
    <div className="h-screen flex flex-col bg-gray-50 dark:bg-gray-900">
      <Header onRefresh={onRefresh} />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar
          onTableSelect={onTableSelect}
          onAddCatalog={onAddCatalog}
          onCatalogHealthClick={onCatalogHealthClick}
          selectedTable={selectedTable}
        />
        <main className="flex-1 overflow-auto">{children}</main>
      </div>
    </div>
  );
}
