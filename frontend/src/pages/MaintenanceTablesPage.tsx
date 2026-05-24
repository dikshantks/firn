import { useMemo } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { MaintenanceTable } from '../components/Health/MaintenanceTable';
import { catalogHealthPath, decodeSegment } from '../lib/paths';

export function MaintenanceTablesPage() {
  const navigate = useNavigate();
  const { catalog: catalogParam } = useParams();
  const [searchParams] = useSearchParams();
  const catalogName = decodeSegment(catalogParam);

  const filter = useMemo(() => {
    const type = searchParams.get('type');
    if (!type) {
      return undefined;
    }
    const valueParam = searchParams.get('value');
    return {
      type,
      value: valueParam ? Number.parseInt(valueParam, 10) : undefined,
    };
  }, [searchParams]);

  if (!catalogName) {
    return null;
  }

  return (
    <div className="h-full overflow-auto bg-white dark:bg-gray-800">
      <MaintenanceTable
        catalogName={catalogName}
        filter={filter}
        onBack={() => navigate(catalogHealthPath(catalogName))}
      />
    </div>
  );
}
