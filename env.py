from __future__ import annotations

from pathlib import Path
from typing import Optional
import os

BASE_DIR = Path(__file__).resolve().parent


def _env_path(name: str) -> Optional[Path]:
	raw = os.environ.get(name)
	if not raw or not str(raw).strip():
		return None
	return Path(str(raw)).expanduser().resolve()


def get_ingestion_data_root() -> Path:
	"""Base data root for all ingestion artifacts."""
	root = _env_path("INGESTION_DATA_ROOT")
	if root is not None:
		return root
	return BASE_DIR / "data"


def get_vector_index_root() -> Path:
	root = _env_path("VECTOR_INDEX_ROOT")
	if root is not None:
		return root
	return get_ingestion_data_root() / "vector_index"


def get_canonical_root() -> Path:
	return get_ingestion_data_root() / "canonical"


def get_chunks_root() -> Path:
	return get_ingestion_data_root() / "chunks"


def get_embeddings_root() -> Path:
	return get_ingestion_data_root() / "embeddings"


def get_releases_root() -> Path:
	root = _env_path("RELEASES_ROOT")
	if root is not None:
		return root
	return get_ingestion_data_root() / "releases"


def get_observability_root() -> Path:
	root = _env_path("OBSERVABILITY_ROOT")
	if root is not None:
		return root
	return get_ingestion_data_root() / "observability"


def get_retrieval_embed_provider() -> str:
	raw = (os.getenv("RETRIEVAL_EMBED_PROVIDER") or "").strip().lower()
	if raw:
		return raw
	model = (os.getenv("OLLAMA_EMBED_MODEL") or "").strip().lower()
	if model and model != "deterministic":
		return "ollama"
	return "deterministic"


def get_ingestion_embed_provider() -> str:
	model = (os.getenv("OLLAMA_EMBED_MODEL") or "").strip().lower()
	if model == "deterministic":
		return "deterministic"
	return "ollama"


def get_ollama_embed_model(default: str = "mxbai-embed-large") -> str:
	raw = (os.getenv("OLLAMA_EMBED_MODEL") or "").strip()
	return raw or default


def get_ollama_base_url(default: str = "http://localhost:11434") -> str:
	raw = (os.getenv("OLLAMA_URL") or "").strip()
	return raw or default


def get_ollama_timeout_s(default: int = 60) -> int:
	raw = (os.getenv("OLLAMA_TIMEOUT_S") or "").strip()
	if not raw:
		return default
	try:
		return int(raw)
	except Exception:
		return default


def get_retrieval_embed_dim(default: int = 16) -> int:
	raw = (os.getenv("RETRIEVAL_EMBED_DIM") or "").strip()
	if not raw:
		return default
	try:
		return int(raw)
	except Exception:
		return default


def get_retrieval_embed_max_chars(default: int = 2000) -> int:
	raw = (os.getenv("RETRIEVAL_EMBED_MAX_CHARS") or "").strip()
	if raw:
		try:
			return int(raw)
		except Exception:
			return default
	raw2 = (os.getenv("OLLAMA_EMBED_MAX_CHARS") or "").strip()
	if raw2:
		try:
			return int(raw2)
		except Exception:
			return default
	return default
