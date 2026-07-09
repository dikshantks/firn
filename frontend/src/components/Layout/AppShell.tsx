import { ReactNode, useState, useEffect } from 'react';
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
  const [sidebarWidth, setSidebarWidth] = useState(256);
  const [isResizing, setIsResizing] = useState(false);

  const selectedTable =
    catalog && namespace && table
      ? {
          catalog: decodeSegment(catalog) ?? catalog,
          namespace: decodeSegment(namespace) ?? namespace,
          table: decodeSegment(table) ?? table,
        }
      : undefined;

  const startResizing = (e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
  };

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      const newWidth = Math.max(160, Math.min(480, e.clientX));
      setSidebarWidth(newWidth);
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    if (isResizing) {
      window.addEventListener('mousemove', handleMouseMove);
      window.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing]);

  return (
    <div className="h-screen flex flex-col bg-gray-50 dark:bg-gray-900">
      <Header onRefresh={onRefresh} />
      <div className="flex flex-1 overflow-hidden select-none">
        <Sidebar onAddCatalog={onAddCatalog} selectedTable={selectedTable} width={sidebarWidth} />
        <div
          className={`w-1 cursor-col-resize hover:bg-iceberg transition-colors h-full shrink-0 ${
            isResizing ? 'bg-iceberg' : 'bg-gray-200 dark:bg-gray-700'
          }`}
          onMouseDown={startResizing}
        />
        <main className="flex-1 overflow-auto select-text">{children}</main>
      </div>
    </div>
  );
}

