import { useState } from 'react';
import {
  File,
  Layers,
  Columns,
  Table,
  Loader2,
  ChevronDown,
  ChevronRight,
} from 'lucide-react';
import { useDataFileInspection, useDataFileSample } from '../../hooks/useIcebergData';

interface DataFileDetailProps {
  catalog: string;
  namespace: string;
  table: string;
  filePath: string;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

export function DataFileDetail({
  catalog,
  namespace,
  table,
  filePath,
}: DataFileDetailProps) {
  const [showSample, setShowSample] = useState(false);
  const [expandedRowGroups, setExpandedRowGroups] = useState<number[]>([]);

  const { data: inspection, isLoading: loadingInspection } = useDataFileInspection(
    catalog,
    namespace,
    table,
    filePath
  );

  const { data: sample, isLoading: loadingSample } = useDataFileSample(
    catalog,
    namespace,
    table,
    showSample ? filePath : '',
    10
  );

  const toggleRowGroup = (index: number) => {
    setExpandedRowGroups((prev) =>
      prev.includes(index)
        ? prev.filter((i) => i !== index)
        : [...prev, index]
    );
  };

  if (loadingInspection) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
      </div>
    );
  }

  if (!inspection) {
    return (
      <div className="text-center py-8 text-gray-500">
        <p>Unable to load file details</p>
      </div>
    );
  }

  const fileName = filePath.split('/').pop() || filePath;

  return (
    <div className="p-4 space-y-4">
      <div>
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
          File Inspector
        </h3>
        <p className="text-sm text-gray-500 font-mono truncate" title={filePath}>
          {fileName}
        </p>
      </div>

      {/* Basic info */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <File className="w-4 h-4" />
            <span className="text-xs">Format</span>
          </div>
          <p className="font-semibold text-gray-900 dark:text-white">
            {inspection.file_format}
          </p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <Layers className="w-4 h-4" />
            <span className="text-xs">Row Groups</span>
          </div>
          <p className="font-semibold text-gray-900 dark:text-white">
            {inspection.num_row_groups || 0}
          </p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <Table className="w-4 h-4" />
            <span className="text-xs">Total Rows</span>
          </div>
          <p className="font-semibold text-gray-900 dark:text-white">
            {(inspection.num_rows || 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
          <div className="flex items-center gap-2 text-gray-600 dark:text-gray-400 mb-1">
            <Columns className="w-4 h-4" />
            <span className="text-xs">Size</span>
          </div>
          <p className="font-semibold text-gray-900 dark:text-white">
            {formatBytes(inspection.file_size_bytes)}
          </p>
        </div>
      </div>

      {/* Compression and encodings */}
      <div className="grid grid-cols-2 gap-3">
        {inspection.compression && (
          <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
            <p className="text-xs text-gray-500 mb-1">Compression</p>
            <p className="font-medium text-gray-900 dark:text-white">
              {inspection.compression}
            </p>
          </div>
        )}
        {inspection.encodings.length > 0 && (
          <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
            <p className="text-xs text-gray-500 mb-1">Encodings</p>
            <div className="flex flex-wrap gap-1">
              {inspection.encodings.slice(0, 5).map((enc, idx) => (
                <span
                  key={idx}
                  className="px-2 py-0.5 bg-gray-200 dark:bg-gray-700 rounded text-xs"
                >
                  {enc}
                </span>
              ))}
              {inspection.encodings.length > 5 && (
                <span className="text-xs text-gray-400">
                  +{inspection.encodings.length - 5} more
                </span>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Schema */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          Schema ({inspection.schema_fields.length} columns)
        </h4>
        <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3 max-h-48 overflow-y-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-gray-500">
                <th className="pb-2">Name</th>
                <th className="pb-2">Type</th>
                <th className="pb-2">Nullable</th>
              </tr>
            </thead>
            <tbody>
              {inspection.schema_fields.map((field: any, idx) => (
                <tr key={idx} className="border-t border-gray-200 dark:border-gray-700">
                  <td className="py-1 font-mono text-xs">{field.name}</td>
                  <td className="py-1 text-gray-600 dark:text-gray-400">{field.type}</td>
                  <td className="py-1">
                    {field.nullable ? (
                      <span className="text-yellow-600">Yes</span>
                    ) : (
                      <span className="text-green-600">No</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Row Groups */}
      {inspection.row_groups.length > 0 && (
        <div>
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            Row Groups
          </h4>
          <div className="space-y-2">
            {inspection.row_groups.map((rg) => (
              <div
                key={rg.row_group_index}
                className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden"
              >
                <div
                  className="flex items-center justify-between px-3 py-2 bg-gray-50 dark:bg-gray-800 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
                  onClick={() => toggleRowGroup(rg.row_group_index)}
                >
                  <div className="flex items-center gap-2">
                    {expandedRowGroups.includes(rg.row_group_index) ? (
                      <ChevronDown className="w-4 h-4 text-gray-500" />
                    ) : (
                      <ChevronRight className="w-4 h-4 text-gray-500" />
                    )}
                    <span className="text-sm font-medium">
                      Row Group {rg.row_group_index}
                    </span>
                  </div>
                  <div className="flex items-center gap-4 text-sm text-gray-500">
                    <span>{rg.num_rows.toLocaleString()} rows</span>
                    <span>{formatBytes(rg.total_byte_size)}</span>
                  </div>
                </div>
                {expandedRowGroups.includes(rg.row_group_index) && (
                  <div className="p-3 text-xs max-h-48 overflow-y-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="text-left text-gray-500">
                          <th className="pb-1">Column</th>
                          <th className="pb-1">Type</th>
                          <th className="pb-1 text-right">Compressed</th>
                          <th className="pb-1 text-right">Uncompressed</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rg.columns.map((col: any, idx) => (
                          <tr key={idx} className="border-t border-gray-100 dark:border-gray-700">
                            <td className="py-1 font-mono">{col.path_in_schema}</td>
                            <td className="py-1 text-gray-500">{col.physical_type}</td>
                            <td className="py-1 text-right">{formatBytes(col.total_compressed_size)}</td>
                            <td className="py-1 text-right">{formatBytes(col.total_uncompressed_size)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Sample data */}
      <div>
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Sample Data
          </h4>
          <button
            onClick={() => setShowSample(!showSample)}
            className="text-sm text-iceberg hover:text-iceberg-dark"
          >
            {showSample ? 'Hide' : 'Show'} Sample
          </button>
        </div>
        {showSample && (
          <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
            {loadingSample ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="w-5 h-5 animate-spin text-gray-400" />
              </div>
            ) : sample ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 dark:bg-gray-800">
                    <tr>
                      {sample.columns.map((col, idx) => (
                        <th
                          key={idx}
                          className="px-3 py-2 text-left font-medium text-gray-600 dark:text-gray-300 whitespace-nowrap"
                        >
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sample.rows.map((row, rowIdx) => (
                      <tr
                        key={rowIdx}
                        className="border-t border-gray-200 dark:border-gray-700"
                      >
                        {row.map((cell, cellIdx) => (
                          <td
                            key={cellIdx}
                            className="px-3 py-2 font-mono text-xs whitespace-nowrap"
                          >
                            {cell === null ? (
                              <span className="text-gray-400">null</span>
                            ) : (
                              String(cell)
                            )}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center py-4 text-gray-500">
                Unable to load sample data
              </div>
            )}
            {sample && (
              <div className="px-3 py-2 bg-gray-50 dark:bg-gray-800 text-xs text-gray-500">
                Showing {sample.sampled_rows} of {sample.total_rows.toLocaleString()} rows
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
