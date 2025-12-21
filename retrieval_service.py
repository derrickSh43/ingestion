"""Retrieval service for the core ingestion pipeline.

This mirrors the Step 11 behavior without relying on ingestion_old.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .embeddings import DeterministicHashEmbeddingProvider, EmbeddingProvider, OllamaEmbeddingProvider
from .env import (
	get_ingestion_embed_provider,
	get_ollama_base_url,
	get_ollama_embed_model,
	get_ollama_timeout_s,
	get_retrieval_embed_dim,
	get_retrieval_embed_max_chars,
	get_retrieval_embed_provider,
	get_vector_index_root,
)
from .releases import ReleaseManager
from .vector_store import VectorStoreAdapter, build_vector_store_adapter


def _default_provider_name() -> str:
	return get_retrieval_embed_provider()


def _ingestion_provider_name() -> str:
	return get_ingestion_embed_provider()


def _build_embedder() -> EmbeddingProvider:
	provider = _default_provider_name()
	if provider == "ollama":
		model = get_ollama_embed_model()
		base_url = get_ollama_base_url()
		timeout_s = get_ollama_timeout_s()
		return OllamaEmbeddingProvider(model=model, base_url=base_url, timeout_s=timeout_s)
	dim = get_retrieval_embed_dim()
	return DeterministicHashEmbeddingProvider(dim=dim)


def _resolve_release_id(release_manager: ReleaseManager, *, domain: str, release_id: Optional[str]) -> str:
	if release_id:
		return str(release_id)
	active = release_manager.get_active_release(domain)
	if not active:
		raise FileNotFoundError("No active release set for domain")
	return active


def _trim_query(text: str) -> str:
	max_chars = get_retrieval_embed_max_chars()
	trimmed = (text or "").strip()
	if len(trimmed) > max_chars:
		return trimmed[:max_chars]
	return trimmed


@dataclass
class RetrievalService:
	"""Wrapper around release resolution + vector store query."""

	release_manager: ReleaseManager
	vector_store: VectorStoreAdapter
	embedder: EmbeddingProvider

	@classmethod
	def from_env(cls) -> "RetrievalService":
		return cls(
			release_manager=ReleaseManager(),
			vector_store=build_vector_store_adapter(root=get_vector_index_root()),
			embedder=_build_embedder(),
		)

	def query(
		self,
		*,
		domain: str,
		query: str,
		filters: Optional[Dict[str, Any]] = None,
		top_k: int = 5,
		release_id: Optional[str] = None,
	) -> Dict[str, Any]:
		if not isinstance(domain, str) or not domain.strip():
			raise ValueError("domain is required")
		if not isinstance(query, str) or not query.strip():
			raise ValueError("query is required")

		resolved_release_id = _resolve_release_id(self.release_manager, domain=domain, release_id=release_id)
		query_text = _trim_query(query)
		if not query_text:
			raise ValueError("query is required")
		vec = self.embedder.embed_texts([query_text])[0]
		results = self.vector_store.query(
			domain=domain,
			release_id=resolved_release_id,
			query_vector=vec,
			filters=filters,
			top_k=int(top_k or 5),
		)
		warnings: list[str] = []
		ingestion_provider = _ingestion_provider_name()
		retrieval_provider = _default_provider_name()
		if ingestion_provider != retrieval_provider:
			warnings.append(
				"Embedding provider mismatch: ingestion uses "
				f"{ingestion_provider}, retrieval uses {retrieval_provider}. "
				"Set RETRIEVAL_EMBED_PROVIDER to match ingestion."
			)
		return {
			"domain": domain,
			"release_id": resolved_release_id,
			"results": results,
			"warnings": warnings,
		}
