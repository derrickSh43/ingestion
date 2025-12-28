# Operator Runbook — Standalone Ingestion

This service captures raw HTML (from URLs or uploaded files), runs a deterministic ingestion pipeline into a **candidate release**, and supports retrieval queries against the **active release**.

## Concepts

- **Domain**: strict subject scope (e.g. `terraform`, `aws`). Domains should not mix.
- **Release**: a snapshot of derived artifacts for a domain (`canonical/`, `chunks/`, `embeddings/`, `vector_index/`).
- **Candidate release**: a new release id produced by ingestion; it is not active until promoted.

## Storage layout (default)

All artifacts live under `INGESTION_DATA_ROOT` (default: `./data`):

- Captures: `data/captures/<domain>/<source_id>.{html,json}`
- Canonical: `data/canonical/<domain>/<release_id>/*.json`
- Chunks: `data/chunks/<domain>/<release_id>/*.json`
- Embeddings: `data/embeddings/<domain>/<release_id>/*.json`
- Vector index: `data/vector_index/<domain>/<release_id>/index.jsonl`
- Releases: `data/releases/<domain>/{active_release.txt,releases/<release_id>/release.json,audit.jsonl}`
- Observability: `data/observability/<domain>/{events.jsonl,counters.json}`

## Workflows

### 1) Capture a single URL

- `POST /ingestion/raw-capture`
- Then ingest using `POST /ingestion/run` with `capture_id=<source_id>`.

### 2) Batch capture URLs (fail-fast by default)

- `POST /ingestion/raw-capture/batch`
- Use `continue_on_error=true` to collect per-item failures.

### 3) Capture + ingest in one call (batch)

- `POST /ingestion/ingest/batch`
- `release_id` is optional; when omitted the server auto-generates a new candidate release id.
- Default behavior is fail-fast; use `continue_on_error=true` to collect per-item failures.
- Use `force=true` to attempt ingest even if a capture is flagged as not usable/quarantined (still fails if ingestion itself errors).

### 4) Upload a file (HTML/text/docs)

- `POST /ingestion/file-capture` (multipart form: `domain`, `source_id`, `file`)
  - Supported: `.html`, `.htm`, `.txt`, `.md`, `.doc`, `.docx` (best-effort; binary `.doc` is lossy)
- Then ingest via `POST /ingestion/run` using `capture_id=<source_id>`.

### 5) Promote / rollback a release

- Promote (recommended): `POST /releases/{domain}/promote` with `{ "release_id": "...", "reason": "..." }`
- Rollback is just promoting a previous release again.
- Audit: `GET /releases/{domain}/audit`

### 6) Retrieval smoke test

- `POST /retrieval/query` with `{domain, query, top_k, filters?, release_id?}`
- If `release_id` is omitted, retrieval uses the domain’s active release.

## Troubleshooting

- **Capture not usable**: check `http_status`, `capture_ok`, and `quarantined` fields in the capture response.
- **No results in retrieval**: ensure you’ve promoted the candidate release for the domain, or pass `release_id` explicitly.
- **Embedding mismatch warning**: set `RETRIEVAL_EMBED_PROVIDER` to match ingestion (`ollama` vs `deterministic`).
- **Release merge**: use `POST /releases/{domain}/merge` with `source_release_ids` (2+) to produce a merged candidate release.
