"""Deterministic HTML distillation into DistilledSection candidates.

Goal: take raw HTML and emit a small set of section candidates suitable for
canonicalization. This is intentionally heuristic and dependency-light (no BS4).

Output schema aligns with docs/schemas/distilled_section.json.

Evidence offsets are character offsets into the *raw HTML* input.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import re

from cleaner import clean_html_text

_CONTAINER_TAGS = ("nav", "footer", "header", "aside")

def _sha256_hex(text: str) -> str:
	h = hashlib.sha256()
	h.update(text.encode("utf-8"))
	return h.hexdigest()

def _make_section_id(domain: str, source_hash: str, kind: str, title: Optional[str], clean_text: str) -> str:
	base = f"{domain}|{source_hash}|{kind}|{title or ''}|{clean_text}"
	return f"sec_{_sha256_hex(base)[:24]}"

def _mask_ranges(raw_html: str, ranges: List[Tuple[int, int]]) -> str:
	if not ranges:
		return raw_html
	chars = list(raw_html)
	for start, end in ranges:
		start = max(0, start)
		end = min(len(chars), end)
		for i in range(start, end):
			if chars[i] != "\n":
				chars[i] = " "
	return "".join(chars)

def _find_container_ranges(raw_html: str) -> List[Tuple[int, int]]:
	ranges: List[Tuple[int, int]] = []
	for tag in _CONTAINER_TAGS:
		pattern = re.compile(rf"<\s*{tag}[^>]*>[\s\S]*?<\s*/\s*{tag}\s*>", re.IGNORECASE)
		for m in pattern.finditer(raw_html):
			ranges.append((m.start(), m.end()))
	if not ranges:
		return []
	ranges.sort()
	merged = [ranges[0]]
	for s, e in ranges[1:]:
		ps, pe = merged[-1]
		if s <= pe:
			merged[-1] = (ps, max(pe, e))
		else:
			merged.append((s, e))
	return merged

_BLOCK_RE = re.compile(
	r"<\s*(h[1-6]|p|li|pre|code|blockquote)\b[^>]*>([\s\S]*?)<\s*/\s*\1\s*>",
	re.IGNORECASE,
)

def _guess_kind(title: Optional[str], text: str) -> str:
	t = (title or "").strip().lower()
	if "example" in t:
		return "example"
	if t.startswith("how to") or "how-to" in t or "howto" in t:
		return "howto"
	if t.startswith("note") or t.startswith("warning") or t.startswith("caution"):
		return "note"
	if "definition" in t:
		return "definition"
	if text.strip().lower().startswith("example:"):
		return "example"
	return "explanation"

def _is_boilerplate(clean_text: str) -> bool:
	s = clean_text.strip().lower()
	if not s:
		return True
	boiler = {
		"home",
		"docs",
		"edit this page",
		"last updated",
	}
	if s in boiler:
		return True
	if len(s) < 3:
		return True
	return False

@dataclass(frozen=True)
class _Block:
	tag: str
	start: int
	end: int
	text: str

def extract_blocks(raw_html: str) -> List[_Block]:
	if not raw_html:
		return []
	mask_ranges = _find_container_ranges(raw_html)
	masked = _mask_ranges(raw_html, mask_ranges)
	blocks: List[_Block] = []
	for m in _BLOCK_RE.finditer(masked):
		tag = m.group(1).lower()
		inner = m.group(2) or ""
		clean = clean_html_text(inner)
		if _is_boilerplate(clean):
			continue
		blocks.append(_Block(tag=tag, start=m.start(), end=m.end(), text=clean))
	seen = set()
	deduped: List[_Block] = []
	for b in blocks:
		key = b.text
		if key in seen:
			continue
		seen.add(key)
		deduped.append(b)
	return deduped

def distill_sections_from_html(raw_html: str, domain: str, source_hash: str) -> List[Dict[str, Any]]:
	blocks = extract_blocks(raw_html)
	sections: List[Dict[str, Any]] = []
	current_title: Optional[str] = None
	current_evidence: List[Dict[str, Any]] = []
	current_parts: List[str] = []
	def flush() -> None:
		nonlocal current_title, current_evidence, current_parts
		if not current_parts:
			current_title = None
			current_evidence = []
			current_parts = []
			return
		clean_text = "\n\n".join(current_parts).strip()
		if not clean_text:
			current_title = None
			current_evidence = []
			current_parts = []
			return
		kind = _guess_kind(current_title, clean_text)
		section_id = _make_section_id(domain=domain, source_hash=source_hash, kind=kind, title=current_title, clean_text=clean_text)
		sec: Dict[str, Any] = {
			"section_id": section_id,
			"domain": domain,
			"kind": kind,
			"title": current_title,
			"clean_text": clean_text,
			"evidence": current_evidence,
		}
		if current_title is None:
			sec.pop("title")
		sections.append(sec)
		current_title = None
		current_evidence = []
		current_parts = []
	for b in blocks:
		if b.tag.startswith("h"):
			flush()
			current_title = b.text
			current_evidence.append({"source_hash": source_hash, "offset": [b.start, b.end]})
			continue
		current_parts.append(b.text)
		current_evidence.append({"source_hash": source_hash, "offset": [b.start, b.end]})
	flush()
	if not sections and blocks:
		clean_text = "\n\n".join([b.text for b in blocks]).strip()
		kind = _guess_kind(None, clean_text)
		section_id = _make_section_id(domain=domain, source_hash=source_hash, kind=kind, title=None, clean_text=clean_text)
		sections = [
			{
				"section_id": section_id,
				"domain": domain,
				"kind": kind,
				"clean_text": clean_text,
				"evidence": [{"source_hash": source_hash, "offset": [b.start, b.end]} for b in blocks],
			}
		]
	return sections

def distill_sections_from_file(raw_html_path: str, domain: str, source_hash: str) -> List[Dict[str, Any]]:
	raw = open(raw_html_path, "r", encoding="utf-8").read()
	return distill_sections_from_html(raw, domain=domain, source_hash=source_hash)
# Moved from ingestion/distiller.py
