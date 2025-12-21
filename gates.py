"""Step 13: CI gating checks for schema + release integrity.

These checks are intentionally dependency-light and deterministic.

They are designed to:
- validate stored JSON artifacts against the repo schemas
- ensure domain + release scoping is consistent across artifacts
- ensure vector index rows reference valid chunk + embedding files

The checks are *best effort* over whatever artifacts exist on disk.
If no artifacts exist, the gates will pass (useful for CI in a clean repo).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import argparse
import json

from .schema_validator import validate_instance
from .releases import get_releases_root
from .vector_store import DEFAULT_INDEX_ROOT
from .chunker import DEFAULT_CHUNKS_ROOT
from .canonicalizer import DEFAULT_CANONICAL_ROOT
from .embeddings import DEFAULT_EMBEDDINGS_ROOT

@dataclass(frozen=True)
class GateIssue:
	code: str
	message: str
	path: Optional[str] = None

class GateError(RuntimeError):
	def __init__(self, issues: List[GateIssue]):
		super().__init__("Gating checks failed")
		self.issues = issues

def _iter_files(root: Path, *, pattern: str) -> Iterable[Path]:
	if not root.exists():
		return
	yield from root.rglob(pattern)

def _read_json(path: Path) -> Dict[str, Any]:
	return json.loads(path.read_text(encoding="utf-8"))

def _domain_release_from_path(root: Path, file_path: Path) -> Tuple[Optional[str], Optional[str]]:
	try:
		rel = file_path.relative_to(root)
	except Exception:
		return None, None
	parts = rel.parts
	if len(parts) < 2:
		return None, None
	return str(parts[0]), str(parts[1])

def _path_from_embedding_ref(embedding_ref: str) -> Optional[Path]:
	if not embedding_ref:
		return None
	if not str(embedding_ref).startswith("file:"):
		return None
	raw = str(embedding_ref)[len("file:") :]
	if not raw:
		return None
	return Path(raw)

def check_release_records(*, releases_root: Path) -> List[GateIssue]:
	issues: List[GateIssue] = []
	for release_json in _iter_files(releases_root, pattern="release.json"):
		try:
			payload = _read_json(release_json)
		except Exception as e:
			issues.append(GateIssue("release_json_invalid", f"Could not parse JSON: {e}", str(release_json)))
			continue
		try:
			validate_instance("release.json", payload)
		except Exception as e:
			issues.append(GateIssue("release_schema_invalid", f"Schema validation failed: {e}", str(release_json)))
		try:
			rel = release_json.relative_to(releases_root)
			parts = rel.parts
			if len(parts) >= 4 and parts[1] == "releases":
				domain_from_path = parts[0]
				rid_from_path = parts[2]
				if str(payload.get("domain")) != str(domain_from_path):
					issues.append(GateIssue("release_domain_mismatch","Release record domain does not match path",str(release_json)))
				if str(payload.get("release_id")) != str(rid_from_path):
					issues.append(GateIssue("release_id_mismatch","Release record release_id does not match path",str(release_json)))
		except Exception:
			pass
	if releases_root.exists():
		for domain_dir in releases_root.iterdir():
			if not domain_dir.is_dir():
				continue
			active_path = domain_dir / "active_release.txt"
			if not active_path.exists():
				continue
			active = active_path.read_text(encoding="utf-8").strip()
			if not active:
				issues.append(GateIssue("active_release_empty", "active_release.txt is empty", str(active_path)))
				continue
			expected = domain_dir / "releases" / active / "release.json"
			if not expected.exists():
				issues.append(GateIssue("active_release_missing","active_release.txt points to a missing release.json",str(active_path)))
	return issues

def check_canonical_store(*, canonical_root: Path) -> List[GateIssue]:
	issues: List[GateIssue] = []
	for p in _iter_files(canonical_root, pattern="*.json"):
		if p.name == "release.json":
			continue
		try:
			payload = _read_json(p)
		except Exception as e:
			issues.append(GateIssue("canonical_json_invalid", f"Could not parse JSON: {e}", str(p)))
			continue
		try:
			validate_instance("canonical_object.json", payload)
		except Exception as e:
			issues.append(GateIssue("canonical_schema_invalid", f"Schema validation failed: {e}", str(p)))
		domain_from_path, rid_from_path = _domain_release_from_path(canonical_root, p)
		if domain_from_path and str(payload.get("domain")) != str(domain_from_path):
			issues.append(GateIssue("canonical_domain_mismatch", "Canonical domain does not match path", str(p)))
		prov = payload.get("provenance") if isinstance(payload.get("provenance"), dict) else {}
		prov_rid = prov.get("release_id")
		if rid_from_path and prov_rid is not None and str(prov_rid) != str(rid_from_path):
			issues.append(GateIssue("canonical_release_mismatch", "Canonical provenance.release_id does not match path", str(p)))
	return issues

def check_chunk_store(*, chunks_root: Path) -> List[GateIssue]:
	issues: List[GateIssue] = []
	for p in _iter_files(chunks_root, pattern="*.json"):
		try:
			payload = _read_json(p)
		except Exception as e:
			issues.append(GateIssue("chunk_json_invalid", f"Could not parse JSON: {e}", str(p)))
			continue
		try:
			validate_instance("chunk.json", payload)
		except Exception as e:
			issues.append(GateIssue("chunk_schema_invalid", f"Schema validation failed: {e}", str(p)))
		domain_from_path, rid_from_path = _domain_release_from_path(chunks_root, p)
		if domain_from_path and str(payload.get("domain")) != str(domain_from_path):
			issues.append(GateIssue("chunk_domain_mismatch", "Chunk domain does not match path", str(p)))
		if rid_from_path and str(payload.get("release_id")) != str(rid_from_path):
			issues.append(GateIssue("chunk_release_mismatch", "Chunk release_id does not match path", str(p)))
		cid_from_path = p.stem
		if str(payload.get("chunk_id")) != str(cid_from_path):
			issues.append(GateIssue("chunk_id_mismatch", "Chunk chunk_id does not match filename", str(p)))
	return issues

def check_vector_index(
	*,
	vector_root: Path,
	chunks_root: Path,
	embeddings_root: Path,
) -> List[GateIssue]:
	issues: List[GateIssue] = []
	for index_path in _iter_files(vector_root, pattern="index.jsonl"):
		domain_from_path, rid_from_path = _domain_release_from_path(vector_root, index_path)
		try:
			lines = index_path.read_text(encoding="utf-8").splitlines()
		except Exception as e:
			issues.append(GateIssue("index_read_failed", f"Could not read index.jsonl: {e}", str(index_path)))
			continue
		for i, line in enumerate(lines, start=1):
			if not line.strip():
				continue
			try:
				row = json.loads(line)
			except Exception as e:
				issues.append(GateIssue("index_row_invalid", f"Line {i}: JSON parse failed: {e}", str(index_path)))
				continue
			chunk_id = str(row.get("chunk_id", ""))
			row_domain = str(row.get("domain", ""))
			row_rid = str(row.get("release_id", ""))
			if domain_from_path and row_domain != str(domain_from_path):
				issues.append(GateIssue("index_domain_mismatch", f"Line {i}: domain mismatch", str(index_path)))
			if rid_from_path and row_rid != str(rid_from_path):
				issues.append(GateIssue("index_release_mismatch", f"Line {i}: release_id mismatch", str(index_path)))
			if not chunk_id:
				issues.append(GateIssue("index_missing_chunk_id", f"Line {i}: missing chunk_id", str(index_path)))
				continue
			ch_path = chunks_root / row_domain / row_rid / f"{chunk_id}.json"
			if not ch_path.exists():
				issues.append(GateIssue("index_missing_chunk_file", f"Line {i}: missing chunk file", str(ch_path)))
			else:
				try:
					ch_payload = _read_json(ch_path)
					validate_instance("chunk.json", ch_payload)
				except Exception as e:
					issues.append(GateIssue("index_chunk_invalid", f"Line {i}: chunk file invalid: {e}", str(ch_path)))
			emb_ref = str(row.get("embedding_ref", ""))
			emb_path = _path_from_embedding_ref(emb_ref)
			if emb_path is None:
				issues.append(GateIssue("index_embedding_ref_invalid", f"Line {i}: unsupported embedding_ref", str(index_path)))
				continue
			if not emb_path.exists():
				issues.append(GateIssue("index_missing_embedding", f"Line {i}: embedding file missing", str(emb_path)))
				continue
			try:
				emb_payload = _read_json(emb_path)
			except Exception as e:
				issues.append(GateIssue("embedding_json_invalid", f"Line {i}: embedding JSON invalid: {e}", str(emb_path)))
				continue
			if str(emb_payload.get("chunk_id")) != chunk_id:
				issues.append(GateIssue("embedding_chunk_id_mismatch", f"Line {i}: embedding chunk_id mismatch", str(emb_path)))
			if str(emb_payload.get("domain")) != row_domain:
				issues.append(GateIssue("embedding_domain_mismatch", f"Line {i}: embedding domain mismatch", str(emb_path)))
			if str(emb_payload.get("release_id")) != row_rid:
				issues.append(GateIssue("embedding_release_id_mismatch", f"Line {i}: embedding release_id mismatch", str(emb_path)))
			vec = emb_payload.get("vector")
			if not isinstance(vec, list) or not all(isinstance(x, (int, float)) for x in vec):
				issues.append(GateIssue("embedding_vector_invalid", f"Line {i}: embedding vector invalid", str(emb_path)))
			try:
				emb_path.relative_to(embeddings_root)
			except Exception:
				issues.append(GateIssue("embedding_outside_root",f"Line {i}: embedding file not under embeddings root",str(emb_path)))
	return issues

def run_all_gates(
	*,
	releases_root: Path,
	canonical_root: Path,
	chunks_root: Path,
	embeddings_root: Path,
	vector_root: Path,
) -> List[GateIssue]:
	issues: List[GateIssue] = []
	issues.extend(check_release_records(releases_root=releases_root))
	issues.extend(check_canonical_store(canonical_root=canonical_root))
	issues.extend(check_chunk_store(chunks_root=chunks_root))
	issues.extend(check_vector_index(vector_root=vector_root, chunks_root=chunks_root, embeddings_root=embeddings_root))
	return issues

def main(argv: Optional[List[str]] = None) -> int:
	parser = argparse.ArgumentParser(description="Run ingestion gating checks (Step 13)")
	parser.add_argument("--releases-root", default=str(get_releases_root()))
	parser.add_argument("--canonical-root", default=str(DEFAULT_CANONICAL_ROOT))
	parser.add_argument("--chunks-root", default=str(DEFAULT_CHUNKS_ROOT))
	parser.add_argument("--embeddings-root", default=str(DEFAULT_EMBEDDINGS_ROOT))
	parser.add_argument("--vector-root", default=str(DEFAULT_INDEX_ROOT))
	args = parser.parse_args(argv)
	issues = run_all_gates(
		releases_root=Path(args.releases_root),
		canonical_root=Path(args.canonical_root),
		chunks_root=Path(args.chunks_root),
		embeddings_root=Path(args.embeddings_root),
		vector_root=Path(args.vector_root),
	)
	if not issues:
		return 0
	print("Ingestion gates failed with issues:")
	for it in issues:
		loc = f" ({it.path})" if it.path else ""
		print(f"- {it.code}: {it.message}{loc}")
	return 2

if __name__ == "__main__":
	raise SystemExit(main())
# Moved from ingestion/gates.py
