# Ingestion Service (Standalone)

Self-contained ingestion + retrieval service with a minimal admin UI. It ingests raw HTML, distills and canonicalizes content, chunks and embeds it (Ollama or deterministic), indexes vectors, and serves retrieval queries and release management endpoints. A small Vite React frontend is included under `frontend/`.

## What this does
- Capture URL content to local storage (raw HTML + metadata).
- Run an ingestion pipeline that produces canonical objects, chunks, embeddings, and a vector index.
- Manage releases per domain (create + promote with audit history).
- Query the active release with vector search.
- Expose a minimal admin UI to run capture, ingestion, retrieval, and promotions.

## High-level flow
1) Capture: fetch URL and persist raw HTML + metadata under `INGESTION_DATA_ROOT`.
2) Distill: extract candidate sections from HTML.
3) Classify: keep instructional sections.
4) Canonicalize: create canonical learning objects and persist them.
5) Chunk: split canonical objects into chunks and persist them.
6) Embed: generate embeddings (Ollama or deterministic) and persist them.
7) Index: upsert chunks into the vector store (local JSONL or adapter).
8) Release: write release metadata and optionally promote as active.
9) Retrieve: embed query text and query the vector store.

## Run it

## Install

Backend (Python):
```
python -m venv .venv
.\.venv\Scripts\activate
pip install fastapi uvicorn
```

Frontend (Node):
```
cd frontend
npm install
```

## Run

Backend:
```
python -m uvicorn ingestion.api:app --reload
```

Frontend:
```
cd frontend
npm install
npm run dev
```

## Environment variables
- `INGESTION_DATA_ROOT`: base directory for all data artifacts (canonical, chunks, embeddings, releases, captures, etc).
- `VECTOR_INDEX_ROOT`: optional override for vector index root.
- `RETRIEVAL_EMBED_PROVIDER`: set to `ollama` to match ingestion when using Ollama embeddings.
- `OLLAMA_EMBED_MODEL`: Ollama model name (e.g. `mxbai-embed-large`).
- `OLLAMA_URL`: Ollama base URL (default `http://localhost:11434`).
- `RELEASES_ROOT`: optional override for release storage.
- `OBSERVABILITY_ROOT`: optional override for observability storage.
- `VECTOR_STORE_ADAPTER`: optional import path for a real vector DB adapter.

## Ollama model setup
1) Install Ollama (https://ollama.com/) and start it.
2) Pull the embedding model:
```
ollama pull mxbai-embed-large
```
3) Ensure these env vars are set:
```
RETRIEVAL_EMBED_PROVIDER=ollama
OLLAMA_EMBED_MODEL=mxbai-embed-large
```

## Backend API
- `POST /ingestion/raw-capture`: fetch and persist a URL capture.
- `POST /ingestion/run`: run the ingestion pipeline (raw_html/raw_html_path/capture_id).
- `POST /ingestion/quarantine`: mark a capture as quarantined.
- `GET /ingestion/{domain}/events`: list observability events.
- `GET /ingestion/{domain}/metrics`: summarize observability events.
- `GET /releases/{domain}`: list releases and active release.
- `GET /releases/{domain}/audit`: list release audit events.
- `POST /releases/{domain}/{release_id}/promote`: promote a release.
- `POST /retrieve`: run a retrieval query.

## Files and responsibilities
- `api.py`: FastAPI app, endpoints, capture storage, observability hooks.
- `pipeline.py`: ingestion pipeline orchestration (distill -> classify -> canonicalize -> chunk -> embed -> index -> release).
- `distiller.py`: HTML block extraction and section distillation.
- `section_classifier.py`: instructional vs non-instructional classification.
- `canonicalizer.py`: canonical learning object creation + persistence.
- `chunker.py`: chunk creation + persistence.
- `embeddings.py`: embedding providers (Ollama + deterministic) + embedding store.
- `vector_store.py`: local JSONL vector store + adapter loader.
- `retrieval_service.py`: retrieval query pipeline + embedder selection.
- `releases.py`: release metadata and promotion audit.
- `observability.py`: event + metrics store.
- `integrity.py`: content hash signing helpers for captures.
- `schema_validator.py`: JSON schema validation helper.
- `env.py`: centralized environment configuration and data roots.
- `frontend/`: minimal Vite React UI wired to the backend.
