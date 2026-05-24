import { Navigate, Route, Routes } from 'react-router-dom';
import { AppLayout } from './layouts/AppLayout';
import { WelcomePage } from './pages/WelcomePage';
import { CatalogHealthPage } from './pages/CatalogHealthPage';
import { MaintenanceTablesPage } from './pages/MaintenanceTablesPage';
import { TableViewPage } from './pages/TableViewPage';

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route index element={<WelcomePage />} />
        <Route path="catalogs/:catalog/health/tables" element={<MaintenanceTablesPage />} />
        <Route path="catalogs/:catalog/health" element={<CatalogHealthPage />} />
        <Route path="catalogs/:catalog/:namespace/:table/:tab" element={<TableViewPage />} />
        <Route path="catalogs/:catalog/:namespace/:table" element={<TableViewPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
