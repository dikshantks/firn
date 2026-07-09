# Fern

Fern is a web application for exploring Apache Iceberg table metadata and operational health.

It helps you inspect snapshots, manifests, files, and storage shape so you can better understand how Iceberg tables evolve over time and where maintenance work is building up.

## Features

- **Catalog Management**: Connect to Hive Metastore or AWS Glue catalogs
- **Snapshot Lineage DAG**: Interactive graph visualization of snapshot history
- **Manifest Tree**: Hierarchical view of manifest lists, manifests, data files, and delete files
- **Data File Inspector**: Deep inspection of Parquet files, including row groups, column stats, and data sampling
- **Puffin Statistics**: View NDV (distinct value counts) from Theta sketches
- **Storage Analytics**: Charts showing file size distribution, format breakdown, and partition stats
- **Operation Timeline**: History of DML operations with record and file counts
- **Optimization Suggestions**: Surface maintenance signals such as old snapshots, small files, and delete-file pressure

## What Fern Is Useful For

- Understanding how a table changed over time through snapshot lineage
- Inspecting manifests and delete files when query behavior becomes harder to explain
- Looking at file counts, file sizes, and partition spread to spot operational debt
- Surfacing maintenance actions such as snapshot expiration or file compaction

## Screenshots

### Snapshot Lineage

![Snapshot lineage](docs/img/Screenshot 2026-06-13 at 12.33.27 PM.png)

Trace how a table evolved over time, inspect individual snapshots, and understand overwrite and append behavior with record- and file-level context.

### Manifest Tree And Delete Files

![Manifest tree](docs/img/Screenshot 2026-06-13 at 12.33.53 PM.png)

Browse the manifest tree for a snapshot and inspect the data files and delete files that shape read behavior.

### Storage Analytics

![Storage analytics](docs/img/Screenshot 2026-06-13 at 12.34.35 PM.png)

Review file count, average file size, format distribution, and partition-level breakdowns to understand storage layout and fragmentation.

### Optimization Suggestions

![Optimization suggestions](docs/img/Screenshot 2026-06-13 at 12.34.18 PM.png)

Highlight tables that may need attention, including snapshot expiration, small-file compaction, and delete-file rewrites.

## Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                     React Frontend                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ Snapshot │ │ Manifest │ │  Data    │ │  Puffin  │           │
│  │   DAG    │ │   Tree   │ │  Files   │ │  Viewer  │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
└────────────────────────────┬────────────────────────────────────┘
                             │ REST API
┌────────────────────────────┴────────────────────────────────────┐
│                     FastAPI Backend                             │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ Catalog  │ │ Metadata │ │ Manifest │ │  Puffin  │           │
│  │ Service  │ │ Service  │ │ Service  │ │ Service  │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
   │  Hive   │          │  AWS    │          │  S3 /   │
   │Metastore│          │  Glue   │          │  MinIO  │
   └─────────┘          └─────────┘          └─────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ for local development
- Node.js 18+ for local development

### Using Docker Compose

1. Start all services:

```bash
cd docker
docker-compose up -d
```

This starts:

- MinIO (S3-compatible storage) at `http://localhost:9000` with console at `http://localhost:9001`
- PostgreSQL for the Hive Metastore backing database
- Hive Metastore at `thrift://localhost:9083`
- Backend API at `http://localhost:8000`
- Frontend at `http://localhost:3000`

2. Generate sample data:

```bash
cd scripts
pip install pyiceberg pyarrow
python generate_sample_data.py
```

3. Open `http://localhost:3000` and add a catalog:

- Name: `local-hive`
- Type: `Hive`
- URI: `thrift://localhost:9083`
- S3 Endpoint: `http://localhost:9000`
- Access Key: `minioadmin`
- Secret Key: `minioadmin`

### Local Development

#### Backend

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env

# Run the server
uvicorn app.main:app --reload
```

API docs are available at `http://localhost:8000/docs`.

#### Frontend

```bash
cd frontend

# Install dependencies
npm install

# Copy environment file
cp .env.example .env

# Run development server
npm run dev
```

The frontend is available at `http://localhost:3000`.

## API Endpoints

### Catalogs

- `POST /api/catalogs` - Register a catalog
- `GET /api/catalogs` - List catalogs
- `GET /api/catalogs/{name}/test` - Test connectivity
- `DELETE /api/catalogs/{name}` - Remove a catalog

### Tables

- `GET /api/tables?catalog=<name>` - List tables
- `GET /api/tables/{namespace}/{table}?catalog=<name>` - Get table metadata
- `GET /api/tables/{namespace}/{table}/metadata?catalog=<name>` - Get raw `metadata.json`

### Snapshots

- `GET /api/tables/{namespace}/{table}/snapshots?catalog=<name>` - Get the snapshot graph
- `GET /api/tables/{namespace}/{table}/snapshots/{id}?catalog=<name>` - Get snapshot details
- `POST /api/tables/{namespace}/{table}/snapshots/compare?catalog=<name>&snapshot1=<id>&snapshot2=<id>` - Compare snapshots

### Manifests

- `GET /api/tables/{namespace}/{table}/snapshots/{id}/manifests?catalog=<name>` - Get the manifest list
- `GET /api/tables/{namespace}/{table}/manifests?catalog=<name>&path=<manifest_path>` - Get manifest entries

### Data Files

- `GET /api/tables/{namespace}/{table}/snapshots/{id}/files?catalog=<name>` - List data files
- `GET /api/tables/{namespace}/{table}/files/inspect?catalog=<name>&path=<file_path>` - Inspect a file
- `GET /api/tables/{namespace}/{table}/files/sample?catalog=<name>&path=<file_path>&rows=10` - Sample file data

### Statistics

- `GET /api/tables/{namespace}/{table}/statistics?catalog=<name>` - List Puffin files
- `GET /api/tables/{namespace}/{table}/statistics/{snapshot_id}?catalog=<name>` - Get decoded statistics

### Analytics

- `GET /api/tables/{namespace}/{table}/analytics/storage?catalog=<name>` - Get storage analytics
- `GET /api/tables/{namespace}/{table}/analytics/history?catalog=<name>` - Get operation history

## Tech Stack

### Backend

- Python 3.11+
- FastAPI + Uvicorn
- pyiceberg
- boto3 for S3 and MinIO access
- fastavro for Avro manifest parsing
- pyarrow for Parquet inspection
- Pydantic v2

### Frontend

- React 18 + TypeScript
- Vite
- React Flow for DAG visualization
- TanStack Query for data fetching
- Tailwind CSS
- Recharts for analytics charts
- Lucide React for icons

## License

MIT
