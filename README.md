# Volmigrate

> **Self-hosted streaming data migration tool from Docker Volumes to Kubernetes PersistentVolumeClaims (PVCs)**

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Volmigrate provides an enterprise-ready, safe, streaming-first approach to discovering Docker volumes, validating prerequisites, and transferring volume contents directly into Kubernetes PVCs with real-time SSE progress reporting and verifiable integrity checks.

---

## 🌟 Key Architecture Principles

1. **Streaming-First:** Direct tar stream through bounded buffers with backpressure. No intermediate archive files on disk.
2. **Storage-Agnostic:** Targets standard Kubernetes PVCs & StorageClasses (compatible with Longhorn, Ceph/RBD, OpenEBS, NFS, hostpath, etc.).
3. **Decoupled Architecture:** Clean separation between the FastAPI Control Plane and the durable Execution Plane (Workers).
4. **Durable State:** PostgreSQL stores connection metadata, migration plans, leases, heartbeats, and audit event logs.
5. **Safe by Default:** No automatic unprompted shutdown of source containers, and destination PVCs are never deleted without explicit operator instruction.
6. **Robust Recoverability:** Atomic job claims with worker leases and heartbeats.

---

## 📐 High-Level Architecture

```text
User / Operator
      |
      v
React + TypeScript UI
      |
    REST / SSE
      |
      v
FastAPI Control Plane (volmigrate-api)
      |
      +---- Connection Manager
      +---- Discovery & Preflight
      +---- Migration Planner
      +---- Migration Orchestrator
      |
      +---- PostgreSQL (State, Events, Job Queue)
      |
      v
Execution Plane Worker (volmigrate-worker)
      |
      +---- Docker Source Adapter
      +---- Streaming Transfer Engine (Tar stream + bounded buffer)
      +---- Kubernetes Destination Adapter
                    |
                    v
             Kubernetes API -> PVC / StorageClass / CSI
```

---

## 🚀 Quick Start (Development with `uv`)

### Prerequisites
- Python 3.12+
- [`uv`](https://docs.astral.sh/uv/) package manager
- Docker Engine with local access (or remote daemon)
- `kubectl` with cluster access (or kubeconfig)

### Setup & Run

```bash
# Clone and setup environment
git clone <repo-url> volmigrate
cd volmigrate

# Install dependencies with uv
uv sync

# Run API server
uv run uvicorn volmigrate.api.main:app --reload --port 8000

# Run worker process
uv run python -m volmigrate.worker.main
```

---

## 📁 Repository Structure

```text
.
├── backend/            # Python backend source code
│   └── volmigrate/
│       ├── adapters/   # Docker source & K8s destination adapters
│       ├── api/        # FastAPI endpoints & routers
│       ├── core/       # Configuration, database, security
│       ├── engine/     # Streaming Tar transfer engine & validation
│       ├── models/     # SQLAlchemy models
│       ├── schemas/    # Pydantic validation schemas
│       ├── services/   # Business logic (discovery, planning, etc.)
│       └── worker/     # Worker daemon, lease manager, executor
├── frontend/           # React + TypeScript Web Dashboard
├── docs/               # Architecture and technical design docs
├── docker-compose.yml  # Local development stack
├── pyproject.toml      # uv-managed Python project definition
└── README.md
```

---

## 📄 License
Apache-2.0
