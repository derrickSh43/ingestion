"""
Embeddings (Step 8).

Provides a pluggable embedding provider interface and a simple file-based
embedding store. For tests (and deterministic runs), a hash-based embedding
provider is included.

Now also includes an OllamaEmbeddingProvider that uses the local Ollama HTTP API.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Protocol, Sequence
import hashlib
import json
import urllib.request
import urllib.error


class EmbeddingProvider(Protocol):
    def embed_texts(self, texts: Sequence[str]) -> List[List[float]]:
        """Return embeddings for each text."""


class EmbeddingStore(Protocol):
    def put(self, *, domain: str, release_id: str, chunk_id: str, vector: List[float]) -> str:
        """Store vector and return an embedding reference string."""


from env import get_embeddings_root

DEFAULT_EMBEDDINGS_ROOT = get_embeddings_root()


def _sha256_hex(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8"))
    return h.hexdigest()


@dataclass
class DeterministicHashEmbeddingProvider:
    """Deterministic embedding provider based on sha256.

    This is NOT semantically meaningful; it's for tests / local determinism.
    """

    dim: int = 16

    def embed_texts(self, texts: Sequence[str]) -> List[List[float]]:
        out: List[List[float]] = []
        for t in texts:
            digest = hashlib.sha256((t or "").encode("utf-8")).digest()
            vec: List[float] = []
            for i in range(self.dim):
                b = digest[i % len(digest)]
                vec.append((b / 255.0) * 2.0 - 1.0)
            out.append(vec)
        return out


@dataclass
class OllamaEmbeddingProvider:
    """
    Real embeddings via Ollama local server.

    Requires:
      - Ollama running (listening on http://localhost:11434)
      - Model pulled (e.g. mxbai-embed-large)

    Uses:
      POST /api/embeddings
      body: {"model": "...", "prompt": "..."}
      resp: {"embedding": [ ... floats ... ]}
    """

    model: str = "mxbai-embed-large"
    base_url: str = "http://localhost:11434"
    timeout_s: int = 60

    def _embed_one(self, text: str) -> List[float]:
        url = f"{self.base_url.rstrip('/')}/api/embeddings"
        payload = {"model": self.model, "prompt": text or ""}
        data = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(
            url=url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
            raise RuntimeError(f"Ollama HTTPError {e.code}: {body}") from e
        except Exception as e:
            raise RuntimeError(f"Failed calling Ollama embeddings at {url}: {e}") from e

        obj = json.loads(raw)
        emb = obj.get("embedding")
        if not isinstance(emb, list) or not emb:
            raise RuntimeError(f"Ollama embeddings response missing 'embedding': {obj}")

        # Ensure floats
        return [float(x) for x in emb]

    def embed_texts(self, texts: Sequence[str]) -> List[List[float]]:
        return [self._embed_one(t) for t in texts]


@dataclass
class FileEmbeddingStore:
    root: Path = DEFAULT_EMBEDDINGS_ROOT

    def put(self, *, domain: str, release_id: str, chunk_id: str, vector: List[float]) -> str:
        dest_dir = self.root / domain / release_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        emb_id = f"emb_{_sha256_hex(json.dumps(vector, separators=(',', ':'), ensure_ascii=False))[:24]}"
        dest = dest_dir / f"{chunk_id}_{emb_id}.json"
        payload = {"chunk_id": chunk_id, "domain": domain, "release_id": release_id, "vector": vector}
        dest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return f"file:{dest.as_posix()}"


def attach_embeddings_for_chunks(
    chunks: List[Dict[str, Any]],
    *,
    provider: EmbeddingProvider,
    store: EmbeddingStore,
) -> List[Dict[str, Any]]:
    """Return new chunk dicts with `embedding_ref` attached. Does not mutate input list."""
    texts = [str(c.get("text", "")) for c in chunks]
    vectors = provider.embed_texts(texts)

    out: List[Dict[str, Any]] = []
    for c, v in zip(chunks, vectors):
        domain = str(c.get("domain"))
        release_id = str(c.get("release_id"))
        chunk_id = str(c.get("chunk_id"))
        emb_ref = store.put(domain=domain, release_id=release_id, chunk_id=chunk_id, vector=v)
        new_c = dict(c)
        new_c["embedding_ref"] = emb_ref
        out.append(new_c)

    return out
