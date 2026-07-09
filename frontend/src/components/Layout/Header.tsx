import { Database, Moon, Sun, RefreshCw, ChevronDown, CheckCircle, XCircle, Activity } from 'lucide-react';
import { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { catalogHealthPath } from '../../lib/paths';
import { useCatalogs } from '../../hooks/useCatalog';

interface HeaderProps {
  onRefresh?: () => void;
}

const GlueLogo = ({ className = "w-4 h-4" }: { className?: string }) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={`${className} text-purple-600 dark:text-purple-400`}
  >
    <rect x="11" y="2" width="2" height="2" rx="0.5" />
    <rect x="7" y="6" width="2" height="2" rx="0.5" />
    <rect x="15" y="6" width="2" height="2" rx="0.5" />
    <path d="M 4 10 H 20 L 14 16 V 19 H 10 V 16 Z" />
    <path d="M 12 19 V 22 M 12 22 L 9 20 M 12 22 L 15 20" />
  </svg>
);

const CatalogLogo = ({ type, className = "w-4 h-4" }: { type: string; className?: string }) => {
  if (type === 'glue') {
    return <GlueLogo className={className} />;
  }
  return <Database className={`${className} text-iceberg`} />;
};

export function Header({ onRefresh }: HeaderProps) {
  const [isDark, setIsDark] = useState(true);
  const [isOpen, setIsOpen] = useState(false);
  const { catalog: currentCatalogName } = useParams();
  const { data: catalogs } = useCatalogs();

  const toggleDarkMode = () => {
    setIsDark(!isDark);
    document.documentElement.classList.toggle('dark');
  };

  const currentCatalog = catalogs?.find(c => c.name === currentCatalogName) || catalogs?.[0];

  return (
    <header className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-4 py-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Database className="w-8 h-8 text-iceberg" />
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-white">
              Fern
            </h1>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Data Lake control center
            </p>
          </div>
        </div>

        <div className="relative flex items-center justify-center">
          {currentCatalog ? (
            <div className="flex items-center gap-2">
              <div
                className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 ${
                  catalogs && catalogs.length > 1 ? 'cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800' : ''
                }`}
                onClick={() => catalogs && catalogs.length > 1 && setIsOpen(!isOpen)}
              >
                <CatalogLogo type={currentCatalog.type} className="w-4 h-4" />
                <span className="text-sm font-medium text-gray-700 dark:text-gray-200">
                  {currentCatalog.name}
                </span>
                {currentCatalog.connected ? (
                  <CheckCircle className="w-4 h-4 text-green-500" />
                ) : (
                  <XCircle className="w-4 h-4 text-red-500" />
                )}
                {catalogs && catalogs.length > 1 && (
                  <ChevronDown className="w-4 h-4 text-gray-500" />
                )}
              </div>
              <Link
                to={catalogHealthPath(currentCatalog.name)}
                className="p-1.5 hover:bg-iceberg/10 rounded-lg transition-colors border border-transparent hover:border-gray-200 dark:hover:border-gray-700"
                title="Catalog Health Dashboard"
              >
                <Activity className="w-4 h-4 text-iceberg" />
              </Link>
            </div>
          ) : (
            <div className="text-sm text-gray-500">No catalogs</div>
          )}

          {isOpen && (
            <>
              <div
                className="fixed inset-0 z-40"
                onClick={() => setIsOpen(false)}
              />
              <div className="absolute top-full mt-2 w-56 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md shadow-lg z-50 py-1">
                {catalogs?.map((cat) => (
                  <div
                    key={cat.name}
                    className="flex items-center justify-between px-3 py-2 hover:bg-gray-100 dark:hover:bg-gray-700"
                  >
                    <Link
                      to={catalogHealthPath(cat.name)}
                      className="flex items-center gap-2 flex-1 text-sm font-medium text-gray-700 dark:text-gray-200 truncate hover:text-iceberg"
                      onClick={() => setIsOpen(false)}
                    >
                      <CatalogLogo type={cat.type} className="w-4 h-4" />
                      <span className="truncate">{cat.name}</span>
                    </Link>
                    <div className="flex items-center gap-1.5 ml-2">
                      {cat.connected ? (
                        <CheckCircle className="w-3.5 h-3.5 text-green-500" />
                      ) : (
                        <XCircle className="w-3.5 h-3.5 text-red-500" />
                      )}
                      <Link
                        to={catalogHealthPath(cat.name)}
                        className="p-1 hover:bg-iceberg/10 rounded transition-colors text-iceberg"
                        title="Catalog Health Dashboard"
                        onClick={() => setIsOpen(false)}
                      >
                        <Activity className="w-3.5 h-3.5" />
                      </Link>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
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
