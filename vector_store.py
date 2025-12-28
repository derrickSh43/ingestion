
from __future__ import annotations
"""Domain-scoped vector store adapter (Step 9).

This is a simple local, file-backed vector store intended for deterministic
behavior and CI-friendly tests.

Key requirements:
- enforce domain/release scoping end-to-end
- prevent cross-domain leakage by construction (separate indexes)
- dependency-light

Storage layout:
    <root>/<domain>/<release_id>/index.jsonl

Each line contains:
    {"chunk_id", "domain", "release_id", "text", "embedding_ref"}

Vectors are loaded from `embedding_ref` (currently supports `file:<path>`)
that points to a JSON payload containing `vector`.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple
import importlib
import json
import math
import os

from env import get_vector_index_root

class VectorStoreAdapter(Protocol):
    def upsert(self, *, domain: str, release_id: str, chunks: List[Dict[str, Any]]) -> None:
        ...

    def query(
        self,
        *,
        domain: str,
        release_id: str,
        query_vector: List[float],
        filters: Optional[Dict[str, Any]] = None,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        ...


_FILTER_KEYS = ("concept_id", "level", "graph_id", "graph_version", "dataset_version", "index_version")


def _opt_filter_value(filters: Optional[Dict[str, Any]], key: str) -> Optional[str]:
    if not filters:
        return None
    v = filters.get(key)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def _matches_filters(row: Dict[str, Any], filters: Optional[Dict[str, Any]]) -> bool:
    if not filters:
        return True
    for k in _FILTER_KEYS:
        required = _opt_filter_value(filters, k)
        if required is None:
            continue
        actual = row.get(k)
        if not isinstance(actual, str) or actual != required:
            return False
    return True


DEFAULT_INDEX_ROOT = get_vector_index_root()


def _split_adapter_path(raw: str) -> Tuple[str, str]:
    if ":" in raw:
        module_name, attr = raw.split(":", 1)
    else:
        module_name, attr = raw.rsplit(".", 1)
    return module_name, attr


def build_vector_store_adapter(*, root: Optional[Path] = None) -> VectorStoreAdapter:
    adapter_path = (os.getenv("VECTOR_STORE_ADAPTER") or "").strip()
    root = root or DEFAULT_INDEX_ROOT
    if not adapter_path:
        return LocalJsonlVectorStore(root=root)
    module_name, attr = _split_adapter_path(adapter_path)
    module = importlib.import_module(module_name)
    target = getattr(module, attr)
    if hasattr(target, "upsert") and hasattr(target, "query") and not callable(target):
        return target
    try:
        return target(root=root)  # type: ignore[call-arg]
    except TypeError:
        return target()  # type: ignore[call-arg]


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


def _load_vector_from_embedding_ref(embedding_ref: str) -> List[float]:
    if not embedding_ref:
        return []
    if embedding_ref.startswith("file:"):
        path = embedding_ref[len("file:") :]
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        vec = payload.get("vector")
        if isinstance(vec, list):
            return [float(x) for x in vec]
        return []
    # Unknown provider
    return []


@dataclass
class LocalJsonlVectorStore:
    root: Path = DEFAULT_INDEX_ROOT

    def _index_path(self, domain: str, release_id: str) -> Path:
        return self.root / domain / release_id / "index.jsonl"

    def upsert(self, *, domain: str, release_id: str, chunks: List[Dict[str, Any]]) -> None:
        if not domain:
            raise ValueError("domain is required")
        if not release_id:
            raise ValueError("release_id is required")

        # Validate chunk scope
        for ch in chunks:
            if str(ch.get("domain")) != domain:
                raise ValueError("chunk domain does not match upsert domain")
            if str(ch.get("release_id")) != release_id:
                raise ValueError("chunk release_id does not match upsert release_id")
            if not ch.get("chunk_id"):
                raise ValueError("chunk_id is required")
            if not ch.get("text"):
                raise ValueError("text is required")
            if not ch.get("embedding_ref"):
                raise ValueError("embedding_ref is required for indexing")

        index_path = self._index_path(domain, release_id)
        index_path.parent.mkdir(parents=True, exist_ok=True)

        # Read existing into a dict (deterministic overwrite by chunk_id)
        existing: Dict[str, Dict[str, Any]] = {}
        if index_path.exists():
            for line in index_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    cid = str(obj.get("chunk_id", ""))
                    if cid:
                        existing[cid] = obj
                except Exception:
                    continue

        for ch in chunks:
            row: Dict[str, Any] = {
                "chunk_id": str(ch["chunk_id"]),
                "domain": domain,
                "release_id": release_id,
                "text": str(ch["text"]),
                "embedding_ref": str(ch["embedding_ref"]),
            }
            # Optional alignment metadata used for filter enforcement.
            for k in _FILTER_KEYS:
                v = ch.get(k)
                if isinstance(v, str) and v.strip():
                    row[k] = v.strip()

            existing[str(ch["chunk_id"])] = row

        # Deterministic write order
        lines = [json.dumps(existing[k], ensure_ascii=False) for k in sorted(existing.keys())]
        index_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    def query(
        self,
        *,
        domain: str,
        release_id: str,
        query_vector: List[float],
        filters: Optional[Dict[str, Any]] = None,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        if not domain:
            raise ValueError("domain is required")
        if not release_id:
            raise ValueError("release_id is required")
        if top_k <= 0:
            return []

        index_path = self._index_path(domain, release_id)
        if not index_path.exists():
            return []

        candidates: List[Tuple[float, Dict[str, Any]]] = []
        for line in index_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not _matches_filters(obj, filters):
                continue
            emb_ref = str(obj.get("embedding_ref", ""))
            vec = _load_vector_from_embedding_ref(emb_ref)
            score = _cosine(query_vector, vec)
            candidates.append((score, obj))

        # Highest cosine first, deterministic tiebreak on chunk_id
        candidates.sort(key=lambda t: (-t[0], str(t[1].get("chunk_id", ""))))
        out: List[Dict[str, Any]] = []
        for score, obj in candidates[:top_k]:
            row_out: Dict[str, Any] = {
                "chunk_id": obj.get("chunk_id"),
                "domain": obj.get("domain"),
                "release_id": obj.get("release_id"),
                "text": obj.get("text"),
                "embedding_ref": obj.get("embedding_ref"),
                "score": float(score),
            }
            for k in _FILTER_KEYS:
                if isinstance(obj.get(k), str):
                    row_out[k] = obj.get(k)
            out.append(row_out)
        return out


@dataclass(frozen=True)
class InMemoryVectorIndex:
    """Optional local-mode acceleration for repeated queries.

    This preloads vectors into memory once, so queries don't perform O(N)
    embedding JSON file reads on every call.

    It is intentionally simple (and deterministic) for v1.
    """

    domain: str
    release_id: str
    items: List[Dict[str, Any]]

    def query(self, *, query_vector: List[float], filters: Optional[Dict[str, Any]] = None, top_k: int = 5) -> List[Dict[str, Any]]:
        if top_k <= 0:
            return []
        candidates: List[Tuple[float, Dict[str, Any]]] = []
        for it in self.items:
            if not _matches_filters(it, filters):
                continue
            score = _cosine(query_vector, it.get("vector") or [])
            candidates.append((score, it))

        candidates.sort(key=lambda t: (-t[0], str(t[1].get("chunk_id", ""))))
        out: List[Dict[str, Any]] = []
        for score, it in candidates[:top_k]:
            row_out: Dict[str, Any] = {
                "chunk_id": it.get("chunk_id"),
                "domain": it.get("domain"),
                "release_id": it.get("release_id"),
                "text": it.get("text"),
                "embedding_ref": it.get("embedding_ref"),
                "score": float(score),
            }
            for k in _FILTER_KEYS:
                if isinstance(it.get(k), str):
                    row_out[k] = it.get(k)
            out.append(row_out)
        return out


def load_in_memory_index(*, root: Path = DEFAULT_INDEX_ROOT, domain: str, release_id: str) -> InMemoryVectorIndex:
    """Load <root>/<domain>/<release_id>/index.jsonl and all referenced vectors into memory."""
    if not domain:
        raise ValueError("domain is required")
    if not release_id:
        raise ValueError("release_id is required")

    index_path = Path(root) / domain / release_id / "index.jsonl"
    if not index_path.exists():
        return InMemoryVectorIndex(domain=domain, release_id=release_id, items=[])

    items: List[Dict[str, Any]] = []
    for line in index_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        emb_ref = str(obj.get("embedding_ref", ""))
        vec = _load_vector_from_embedding_ref(emb_ref)
        it: Dict[str, Any] = {
            "chunk_id": obj.get("chunk_id"),
            "domain": obj.get("domain"),
            "release_id": obj.get("release_id"),
            "text": obj.get("text"),
            "embedding_ref": obj.get("embedding_ref"),
            "vector": vec,
        }
        for k in _FILTER_KEYS:
            if isinstance(obj.get(k), str):
                it[k] = obj.get(k)
        items.append(it)


