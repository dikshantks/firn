import { Database, Moon, Sun, RefreshCw } from 'lucide-react';
import { useState } from 'react';

interface HeaderProps {
  onRefresh?: () => void;
}

export function Header({ onRefresh }: HeaderProps) {
  const [isDark, setIsDark] = useState(false);

  const toggleDarkMode = () => {
    setIsDark(!isDark);
    document.documentElement.classList.toggle('dark');
  };

  return (
    <header className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-4 py-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Database className="w-8 h-8 text-iceberg" />
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-white">
              Iceberg Visualizer
            </h1>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Apache Iceberg Metadata Explorer
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          {onRefresh && (
            <button
              onClick={onRefresh}
              className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
              title="Refresh"
            >
              <RefreshCw className="w-5 h-5 text-gray-600 dark:text-gray-300" />
            </button>
          )}
          <button
            onClick={toggleDarkMode}
            className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
            title="Toggle dark mode"
          >
            {isDark ? (
              <Sun className="w-5 h-5 text-gray-600 dark:text-gray-300" />
            ) : (
              <Moon className="w-5 h-5 text-gray-600 dark:text-gray-300" />
            )}
          </button>
        </div>
      </div>
    </header>
  );
}
