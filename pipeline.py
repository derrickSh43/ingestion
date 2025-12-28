from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import hashlib

from canonicalizer import canonicalize_sections
from chunker import chunk_canonical_objects, persist_chunks
from distiller import distill_sections_from_html
from embeddings import (
	DeterministicHashEmbeddingProvider,
	EmbeddingProvider,
	FileEmbeddingStore,
	OllamaEmbeddingProvider,
	attach_embeddings_for_chunks,
)
from env import (
	get_embeddings_root,
	get_ingestion_embed_provider,
	get_ollama_base_url,
	get_ollama_embed_model,
	get_ollama_timeout_s,
)
from releases import ReleaseManager
from section_classifier import filter_instructional_sections
from vector_store import build_vector_store_adapter


def _sha256_hex(text: str) -> str:
	h = hashlib.sha256()
	h.update((text or "").encode("utf-8"))
	return h.hexdigest()


def _build_ingestion_embedder() -> EmbeddingProvider:
	provider = get_ingestion_embed_provider()
	if provider == "ollama":
		return OllamaEmbeddingProvider(
			model=get_ollama_embed_model(),
			base_url=get_ollama_base_url(),
			timeout_s=get_ollama_timeout_s(),
		)
	return DeterministicHashEmbeddingProvider()


@dataclass
class IngestionRunResult:
	status: str
	domain: str
	release_id: str
	release: Dict[str, Any]
	counts: Dict[str, int]


def run_ingestion(
	*,
	domain: str,
	source_id: str,
	release_id: str,
	raw_html: str,
	created_by: Optional[str] = None,
	write_release: bool = True,
) -> IngestionRunResult:
	if not domain or not domain.strip():
		raise ValueError("domain is required")
	if not source_id or not source_id.strip():
		raise ValueError("source_id is required")
	if not release_id or not release_id.strip():
		raise ValueError("release_id is required")
	if not raw_html or not raw_html.strip():
		raise ValueError("raw_html is required")

	source_hash = _sha256_hex(raw_html)
	sections = distill_sections_from_html(raw_html, domain=domain, source_hash=source_hash)
	kept_sections, _dropped = filter_instructional_sections(sections)
	canonical = canonicalize_sections(
		kept_sections,
		domain=domain,
		source_id=source_id,
		release_id=release_id,
		persist=True,
	)
	chunks = chunk_canonical_objects(canonical, domain=domain, release_id=release_id)
	persist_chunks(chunks)

	embedder = _build_ingestion_embedder()
	store = FileEmbeddingStore(root=get_embeddings_root())
	chunks_with_embeddings = attach_embeddings_for_chunks(chunks, provider=embedder, store=store)

	vector_store = build_vector_store_adapter()
	vector_store.upsert(domain=domain, release_id=release_id, chunks=chunks_with_embeddings)

	release_meta: Dict[str, Any] = {}
	if write_release:
		release_manager = ReleaseManager()
		release_meta = release_manager.create_release(
			domain=domain,
			release_id=release_id,
			created_by=created_by,
			payload={
				"source_id": source_id,
				"source_hash": source_hash,
				"stats": {
					"sections_total": len(sections),
					"sections_kept": len(kept_sections),
					"canonical_objects": len(canonical),
					"chunks": len(chunks),
				},
			},
		)

	return IngestionRunResult(
		status="ok",
		domain=domain,
		release_id=release_id,
		release=release_meta,
		counts={
			"sections_total": len(sections),
			"sections_kept": len(kept_sections),
			"canonical_objects": len(canonical),
			"chunks": len(chunks),
			"embeddings": len(chunks_with_embeddings),
		},
	)
