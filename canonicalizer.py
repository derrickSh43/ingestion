"""Canonicalizer (Step 7).

Turns distilled sections into immutable-ish CanonicalLearningObjects.

Design goals:
- deterministic IDs (stable across runs for same inputs)
- provenance included (source_id, release_id)
- optional local filesystem persistence for v1

Schema target: docs/schemas/canonical_object.json
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import hashlib
import json

from env import get_canonical_root

DEFAULT_CANONICAL_ROOT = get_canonical_root()

def _sha256_hex(text: str) -> str:
	h = hashlib.sha256()
	h.update(text.encode("utf-8"))
	return h.hexdigest()

def _canonical_id(domain: str, release_id: str, source_id: str, section_id: str) -> str:
	base = f"{domain}|{release_id}|{source_id}|{section_id}"
	return f"clo_{_sha256_hex(base)[:24]}"

def _title_from_section(section: Dict[str, Any]) -> str:
	title = section.get("title")
	if isinstance(title, str) and title.strip():
		return title.strip()
	clean_text = str(section.get("clean_text", "")).strip()
	first = next((ln.strip() for ln in clean_text.splitlines() if ln.strip()), "Untitled")
	return first[:120]

def _body_from_clean_text(clean_text: str) -> List[str]:
	parts = [p.strip() for p in clean_text.split("\n\n")]
	return [p for p in parts if p]

def canonicalize_sections(
	sections: List[Dict[str, Any]],
	*,
	domain: str,
	source_id: str,
	release_id: str,
	storage_root: Optional[Path] = None,
	persist: bool = False,
) -> List[Dict[str, Any]]:
	if storage_root is None:
		base = DEFAULT_CANONICAL_ROOT
	else:
		base = Path(storage_root)
	out: List[Dict[str, Any]] = []
	ordered = sorted(sections, key=lambda s: str(s.get("section_id", "")))
	for sec in ordered:
		section_id = str(sec.get("section_id", ""))
		clean_text = str(sec.get("clean_text", ""))
		clo_id = _canonical_id(domain=domain, release_id=release_id, source_id=source_id, section_id=section_id)
		title = _title_from_section(sec)
		body = _body_from_clean_text(clean_text)
		clo: Dict[str, Any] = {
			"id": clo_id,
			"domain": domain,
			"title": title,
			"body": body,
			"concepts": [],
			"provenance": {
				"source_id": source_id,
				"release_id": release_id,
			},
		}
		out.append(clo)
		if persist:
			dest_dir = base / domain / release_id
			dest_dir.mkdir(parents=True, exist_ok=True)
			dest_path = dest_dir / f"{clo_id}.json"
			dest_path.write_text(json.dumps(clo, ensure_ascii=False, indent=2), encoding="utf-8")
	return out
# Moved from ingestion/canonicalizer.py
