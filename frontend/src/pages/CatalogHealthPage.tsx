import { useNavigate, useParams } from 'react-router-dom';
import { CatalogHealthDashboard } from '../components/Health/CatalogHealthDashboard';
import { catalogHealthTablesPath, decodeSegment, homePath } from '../lib/paths';

export function CatalogHealthPage() {
  const navigate = useNavigate();
  const { catalog: catalogParam } = useParams();
  const catalogName = decodeSegment(catalogParam);

  if (!catalogName) {
    return null;
  }

  return (
    <div className="h-full overflow-auto bg-white dark:bg-gray-800">
      <CatalogHealthDashboard
        catalogName={catalogName}
        onBack={() => navigate(homePath())}
        onViewTables={(filter) => navigate(catalogHealthTablesPath(catalogName, filter))}
      />
    </div>
  );
}
