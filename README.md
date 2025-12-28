# Ingestion Service (Standalone)

Self-contained ingestion + retrieval service with a minimal admin UI. It ingests raw HTML, distills and canonicalizes content, chunks and embeds it (Ollama or deterministic), indexes vectors, and serves retrieval queries and release management endpoints. A small Vite React frontend is included under `frontend/`.

## What this does
- Capture URL content to local storage (raw HTML + metadata).
- Run an ingestion pipeline that produces canonical objects, chunks, embeddings, and a vector index.
- Manage releases per domain (create + promote with audit history).
- Query the active release with vector search.
- Expose a minimal admin UI to run capture, ingestion, retrieval, and promotions.

Runbook: `docs/INGESTION_RUNBOOK.md`

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
python -m uvicorn api:app --host 127.0.0.1 --port 8002 --reload
```

Frontend:
```
cd frontend
npm install
npm run dev
```

Frontend config:
- `frontend/.env`: set `VITE_API_BASE_URL=http://localhost:8002` to point the UI at the backend.

Useful URLs:
- Backend docs: `http://127.0.0.1:8002/docs`
- Frontend UI: `http://127.0.0.1:5174/`

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
- `GET /domains`: list domains with any local artifacts.
- `POST /ingestion/raw-capture`: fetch and persist a URL capture.
- `POST /ingestion/raw-capture/batch`: capture multiple URLs (fail-fast by default).
- `POST /ingestion/file-capture`: upload a file (.html/.txt/.md/.doc/.docx) and persist it as a capture.
- `POST /ingestion/run`: run the ingestion pipeline (raw_html/raw_html_path/capture_id).
- `POST /ingestion/run/batch`: run ingestion for multiple inputs into one release (release_id optional; fail-fast by default).
- `POST /ingestion/ingest/batch`: convenience endpoint for capture+run in one call (release_id optional; fail-fast by default).
- `POST /ingestion/quarantine`: mark a capture as quarantined.
- `GET /ingestion/{domain}/events`: list observability events.
- `GET /ingestion/{domain}/metrics`: summarize observability events.
- `GET /releases/{domain}`: list releases and active release.
- `GET /releases/{domain}/audit`: list release audit events.
- `POST /releases/{domain}/merge`: merge multiple releases into a new candidate release.
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


# Ingestion Service: Learning Guide

## What This Service Does

This is a complete **capture → process → search** pipeline for learning content.

You'll learn to:
1. Capture a URL and store its HTML
2. Process it through a pipeline (distill, filter, chunk, embed)
3. Promote a "release" (make it searchable)
4. Retrieve answers from the active release

Think of it like: collect web pages → break them into pieces → make them searchable → ask questions.

---

## Before You Start

### Required
- **Python** (3.8+) with `venv`
- **Node.js + npm** (for the admin interface)
- **A folder for data** (you'll tell the system where to store everything)

### Optional (for real embeddings)
- **Ollama** running locally with an embedding model

If you skip Ollama, the system will use deterministic embeddings (good for testing).

---

## Setup

### 1. Get the Code

```bash
git clone <repo-url>
cd ingestion
```

### 2. Backend Setup

```bash
# Create a Python virtual environment
python -m venv .venv

# Activate it
# On Windows:
.\.venv\Scripts\activate
# On Mac/Linux:
source .venv/bin/activate

# Install dependencies
pip install fastapi uvicorn
```

### 3. Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

The UI will start at `http://localhost:5173`

### 4. Set Up Your Data Folder

Create a folder where the system will store everything (raw captures, processed data, vector index, etc.). Then set these environment variables:

```bash
# Windows (set these in your terminal or .env file)
set INGESTION_DATA_ROOT=D:\AI cloud class\data
set VECTOR_INDEX_ROOT=D:\AI cloud class\data\vector_index
set RETRIEVAL_EMBED_PROVIDER=ollama
set OLLAMA_EMBED_MODEL=mxbai-embed-large
```

```bash
# Mac/Linux
export INGESTION_DATA_ROOT=/path/to/your/data
export VECTOR_INDEX_ROOT=/path/to/your/data/vector_index
export RETRIEVAL_EMBED_PROVIDER=ollama
export OLLAMA_EMBED_MODEL=mxbai-embed-large
```

**Just getting started?** Skip the Ollama ones. The system will work fine without them.

### 5. Start the Backend

```bash
python -m uvicorn ingestion.api:app --reload
```

The API will start at `http://localhost:8000`

---

## The Workflow

### Step 1: Capture a URL

**What it does:** Downloads a web page and stores the raw HTML.

**In the UI:**
1. Open `http://localhost:5173`
2. Paste a URL
3. Click "Capture"

**What to verify:**
- A new folder appears in your data directory
- The HTML file matches the page you captured

**Behind the scenes:**
- Endpoint: `POST /ingestion/raw-capture`
- The system stores the raw HTML + metadata on disk

---

### Step 2: Ingest & Process

**What it does:** Transforms raw HTML into searchable pieces.

The pipeline runs these stages in order:

| Stage | What it does | Output |
|-------|-------------|--------|
| **Distill** | Extracts text blocks from HTML | Sections |
| **Classify** | Filters instructional vs non-instructional content | Labeled sections |
| **Canonicalize** | Converts sections into structured learning objects | Canonical objects |
| **Chunk** | Splits objects into search-sized pieces | Chunks (500 tokens each) |
| **Embed** | Converts text to vectors for search | Embeddings |
| **Index** | Organizes embeddings for fast retrieval | Vector index |

**In the UI:**
1. Select a capture
2. Click "Run Ingestion"
3. Wait for it to complete

**What to verify:**
Look in your data folder — you should see:
```
data/
├── captures/          (raw HTML)
├── distilled/         (extracted sections)
├── canonical/         (structured objects)
├── chunks/            (pieces ready to embed)
├── embeddings/        (vector files)
└── vector_index/      (search index)
```

---

### Step 3: Manage Releases

**What it does:** A "release" is a snapshot you promote to make it active for searching.

**In the UI:**
1. Go to "Releases" section
2. View all releases for a domain
3. Click "Promote" to make one active

**Behind the scenes:**
- Only the **active release** can be searched
- Audit trail tracks all promotions

---

### Step 4: Retrieve (Search)

**What it does:** Searches the active release with a natural language query.

**In the UI:**
1. Type your question
2. Click "Search"
3. Get back ranked results

**Behind the scenes:**
- Your query gets embedded (converted to a vector)
- The system finds similar chunks in the vector index
- Results are ranked by relevance

---

## Project Structure (What Each File Does)

### The Main Files

**`api.py`** — The front door. Handles all HTTP requests, captures URLs, stores data, logs events.

**`pipeline.py`** — The assembly line. Orchestrates all processing stages in order.

**`env.py`** — The wiring harness. Centralizes all configuration and data paths.

### Pipeline Stages

**`distiller.py`** — Extracts text blocks from raw HTML.

**`section_classifier.py`** — Filters out non-instructional content.

**`canonicalizer.py`** — Converts sections into structured learning objects.

**`chunker.py`** — Splits objects into bite-sized pieces for embedding.

**`embeddings.py`** — Converts text to vectors (using Ollama or deterministic fallback).

**`vector_store.py`** — Stores and queries the vector index.

### Supporting Files

**`retrieval_service.py`** — Handles search queries against the active release.

**`releases.py`** — Manages release metadata and promotion audits.

**`observability.py`** — Logs events and metrics.

**`integrity.py`** — Verifies data integrity and content hashes.

**`schema_validator.py`** — Validates JSON data at each stage.

**`gates.py`** — Input validation and safety checks.

**Mental model:** Each stage has one job, saves its output, then passes to the next stage.

---

## Troubleshooting

### "Data folder doesn't exist"
Create the folder and set `INGESTION_DATA_ROOT` to its path.

### "Ollama connection failed"
Either:
1. Start Ollama: `ollama serve`
2. Or remove the Ollama environment variables (the system will fall back to deterministic embeddings)

### "Ingestion didn't produce chunks"
Check that the content passed the classifier (Step 2, Classify stage). Some pages may be filtered out if they're not instructional.

### "Search returns no results"
Make sure you've promoted a release to "active" before searching.

---

## Next Steps

1. **Understand the pipeline** — Run a capture and watch the data folder fill up at each stage
2. **Read the code** — Start with `api.py`, then follow to `pipeline.py`
3. **Modify a stage** — Try tweaking the distiller or classifier to see how it changes output
4. **Build on it** — Add custom metadata, change chunking strategy, etc.

---

## Questions?

The best way to learn is to:
- Capture a real page
- Watch the folders fill up
- Search for something and see what comes back
- Read the code that made it happen
- `observability.py`: event + metrics store.
- `integrity.py`: content hash signing helpers for captures.
- `schema_validator.py`: JSON schema validation helper.
- `env.py`: centralized environment configuration and data roots.
- `frontend/`: minimal Vite React UI wired to the backend.
