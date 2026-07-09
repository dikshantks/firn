# Contributing to Fern

Thank you for your interest in contributing to Fern! Fern is an open-source project, and we welcome contributions from the community.

---

## Codebase Architecture Overview

Fern is split into a React frontend and a Python backend:

1. **Backend (`/backend`)**:
   - Built with **FastAPI** and **pyiceberg**.
   - Handles Iceberg catalog connection registry, metadata inspection, background catalog health scans, and optimization job queuing.
   - Exposes REST APIs and a Server-Sent Events (SSE) channel for real-time progress.
   - Can run with an in-memory/SQLite database for local development or MySQL for high availability.

2. **Frontend (`/frontend`)**:
   - Built with **React 18**, **TypeScript**, and **Vite**.
   - Uses **React Flow** for the interactive snapshot lineage DAG.
   - Uses **Tailwind CSS** for UI styling.
   - Employs **TanStack Query** for backend state synchronization.

---

## Local Development & Setup

Please refer to the [Quick Start and Local Development guide in the README.md](README.md#quick-start) to set up your environment, launch the services via Docker Compose, and generate mock data.

---

## Verification & Testing

Before submitting a Pull Request, please verify your changes pass both backend tests and frontend typechecks.

### Backend Verification

1. Navigate to the `backend` directory:
   ```bash
   cd backend
   ```
2. Activate your virtual environment and run the test suite:
   ```bash
   pytest
   ```

### Frontend Verification

1. Navigate to the `frontend` directory:
   ```bash
   cd frontend
   ```
2. Run TypeScript typecheck to verify there are no compilation errors:
   ```bash
   npx tsc --noEmit
   ```
3. Run the linter:
   ```bash
   npm run lint
   ```

---

## Pull Request Guidelines

- **Keep PRs focused**: Each PR should address a single feature or bug fix.
- **Explain the named requirement**: When introducing new abstractions or configuration parameters, make sure they serve a specific, documented requirement.
- **Redact secrets**: Do not log or expose raw AWS credentials, session tokens, or catalog secrets in logs, API responses, or commits. Use `_sanitize_properties` helpers.
- **Document changes**: If you add new API endpoints or environment variables, update the README.md or relevant documentation.
