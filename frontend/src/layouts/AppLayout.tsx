import { useEffect, useState } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { AppShell } from '../components/Layout/AppShell';
import { CatalogForm } from '../components/Catalog/CatalogForm';

export function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();
  const [showCatalogForm, setShowCatalogForm] = useState(false);

  useEffect(() => {
    const state = location.state as { openCatalogForm?: boolean } | null;
    if (state?.openCatalogForm) {
      setShowCatalogForm(true);
      navigate(location.pathname + location.search, { replace: true, state: null });
    }
  }, [location.pathname, location.search, location.state, navigate]);

  const handleRefresh = () => {
    queryClient.invalidateQueries();
  };

  return (
    <>
      <AppShell onAddCatalog={() => setShowCatalogForm(true)} onRefresh={handleRefresh}>
        <Outlet />
      </AppShell>

      {showCatalogForm && <CatalogForm onClose={() => setShowCatalogForm(false)} />}
    </>
  );
}
