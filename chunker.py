"""Chunking (Step 8).

Turns CanonicalLearningObjects into small, deterministic chunks suitable for
embedding + indexing.

Chunk schema: docs/schemas/chunk.json

Design goals:
- deterministic chunk boundaries (given same inputs + params)
- deterministic chunk_id
- dependency-light
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import hashlib
import json
import re

from .env import get_chunks_root

DEFAULT_CHUNKS_ROOT = get_chunks_root()

def _sha256_hex(text: str) -> str:
	h = hashlib.sha256()
	h.update(text.encode("utf-8"))
	return h.hexdigest()

def _chunk_id(domain: str, release_id: str, clo_id: str, chunk_index: int, text: str) -> str:
	base = f"{domain}|{release_id}|{clo_id}|{chunk_index}|{text}"
	return f"chk_{_sha256_hex(base)[:24]}"

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[\.!\?])\s+")

def _split_long_paragraph(text: str, max_chars: int) -> List[str]:
	t = (text or "").strip()
	if len(t) <= max_chars:
		return [t] if t else []
	sentences = [s.strip() for s in _SENTENCE_SPLIT_RE.split(t) if s.strip()]
	if len(sentences) <= 1:
		return [t[i : i + max_chars].strip() for i in range(0, len(t), max_chars) if t[i : i + max_chars].strip()]
	parts: List[str] = []
	cur: List[str] = []
	cur_len = 0
	for s in sentences:
		add = (" " if cur else "") + s
		if cur and (cur_len + len(add) > max_chars):
			parts.append("".join(cur).strip())
			cur = [s]
			cur_len = len(s)
		else:
			cur.append(add)
			cur_len += len(add)
	if cur:
		parts.append("".join(cur).strip())
	final: List[str] = []
	for p in parts:
		if len(p) <= max_chars:
			final.append(p)
		else:
			final.extend([p[i : i + max_chars].strip() for i in range(0, len(p), max_chars) if p[i : i + max_chars].strip()])
	return final

def chunk_canonical_object(
	clo: Dict[str, Any],
	*,
	domain: str,
	release_id: str,
	max_chars: int = 800,
) -> List[Dict[str, Any]]:
	clo_id = str(clo.get("id", ""))
	body = clo.get("body")
	paragraphs: List[str] = []
	if isinstance(body, list):
		paragraphs = [str(p).strip() for p in body if str(p).strip()]
	else:
		paragraphs = [str(body).strip()] if body else []
	units: List[str] = []
	for p in paragraphs:
		units.extend(_split_long_paragraph(p, max_chars=max_chars))
	chunks: List[Dict[str, Any]] = []
	cur: List[str] = []
	cur_len = 0
	chunk_index = 0
	def _opt_str_field(key: str) -> Optional[str]:
		v = clo.get(key)
		if isinstance(v, str) and v.strip():
			return v.strip()
		return None
	concept_id = _opt_str_field("concept_id")
	level = _opt_str_field("level")
	graph_id = _opt_str_field("graph_id")
	graph_version = _opt_str_field("graph_version")
	dataset_version = _opt_str_field("dataset_version")
	index_version = _opt_str_field("index_version")
	def flush() -> None:
		nonlocal cur, cur_len, chunk_index
		if not cur:
			return
		text = "\n\n".join(cur).strip()
		if not text:
			cur = []
			cur_len = 0
			return
		cid = _chunk_id(domain=domain, release_id=release_id, clo_id=clo_id, chunk_index=chunk_index, text=text)
		ch: Dict[str, Any] = {
			"chunk_id": cid,
			"domain": domain,
			"release_id": release_id,
			"text": text,
		}
		if concept_id:
			ch["concept_id"] = concept_id
		if level:
			ch["level"] = level
		if graph_id:
			ch["graph_id"] = graph_id
		if graph_version:
			ch["graph_version"] = graph_version
		if dataset_version:
			ch["dataset_version"] = dataset_version
		if index_version:
			ch["index_version"] = index_version
		chunks.append(ch)
		chunk_index += 1
		cur = []
		cur_len = 0
	for u in units:
		if not u:
			continue
		add_len = len(u) + (2 if cur else 0)
		if cur and (cur_len + add_len > max_chars):
			flush()
		cur.append(u)
		cur_len += add_len
	flush()
	return chunks

def chunk_canonical_objects(
	clos: List[Dict[str, Any]],
	*,
	domain: str,
	release_id: str,
	max_chars: int = 800,
) -> List[Dict[str, Any]]:
	ordered = sorted(clos, key=lambda c: str(c.get("id", "")))
	out: List[Dict[str, Any]] = []
	for clo in ordered:
		out.extend(chunk_canonical_object(clo, domain=domain, release_id=release_id, max_chars=max_chars))
	return out

def persist_chunks(chunks: List[Dict[str, Any]], storage_root: Optional[Path] = None) -> List[str]:
	base = DEFAULT_CHUNKS_ROOT if storage_root is None else Path(storage_root)
	written: List[str] = []
	for ch in chunks:
		domain = str(ch.get("domain"))
		release_id = str(ch.get("release_id"))
		chunk_id = str(ch.get("chunk_id"))
		dest_dir = base / domain / release_id
		dest_dir.mkdir(parents=True, exist_ok=True)
		dest = dest_dir / f"{chunk_id}.json"
		dest.write_text(json.dumps(ch, ensure_ascii=False, indent=2), encoding="utf-8")
		written.append(str(dest.as_posix()))
	return written
# Moved from ingestion/chunker.py
