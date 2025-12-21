"""Deterministic section classification and drop rules.

Step 5 goal (per ingestion plan): classify distilled sections as instructional vs
non-instructional and drop non-instructional ones.

Important: DistilledSection schema has `additionalProperties: false`, so we do
not annotate the section dicts in-place. Instead we return filtered lists and
(optional) side-channel metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import re

@dataclass(frozen=True)
class Classification:
	is_instructional: bool
	score: float
	reasons: Tuple[str, ...]

_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")

_NON_INSTRUCTIONAL_PHRASES = {
	"table of contents",
	"toc",
	"subscribe",
	"sign in",
	"log in",
	"login",
	"cookie policy",
	"privacy policy",
	"terms of service",
	"copyright",
	"all rights reserved",
	"newsletter",
	"advertisement",
	"sponsored",
	"share this",
	"edit this page",
	"last updated",
}

_NON_INSTRUCTIONAL_HINTS = {
	"next",
	"previous",
	"page",
	"breadcrumbs",
	"cookie",
	"consent",
	"tracking",
	"analytics",
	"github",
	"twitter",
	"linkedin",
}

_INSTRUCTIONAL_VERBS = {
	"run",
	"use",
	"create",
	"configure",
	"deploy",
	"install",
	"set",
	"enable",
	"disable",
	"define",
	"apply",
	"initialize",
	"init",
}

def _normalize(s: str) -> str:
	return " ".join((s or "").strip().lower().split())

def classify_section(section: Dict[str, Any]) -> Classification:
	kind = _normalize(str(section.get("kind", "")))
	title = _normalize(section.get("title") or "")
	text = _normalize(str(section.get("clean_text", "")))
	reasons: List[str] = []
	score = 0.0
	if not text:
		return Classification(False, -10.0, ("empty_text",))
	if kind in {"howto", "example", "definition"}:
		score += 3.0
		reasons.append(f"kind:{kind}")
	elif kind in {"note", "explanation"}:
		score += 1.0
		reasons.append(f"kind:{kind}")
	for phrase in _NON_INSTRUCTIONAL_PHRASES:
		if phrase in title or phrase in text:
			score -= 6.0
			reasons.append(f"non_instr_phrase:{phrase}")
	for hint in _NON_INSTRUCTIONAL_HINTS:
		if hint in title or hint in text:
			score -= 1.0
			reasons.append(f"non_instr_hint:{hint}")
	if "table of contents" in title or text.startswith("table of contents"):
		score -= 8.0
		reasons.append("toc")
	words = [w.lower() for w in _WORD_RE.findall(text)]
	verb_hits = sum(1 for w in words if w in _INSTRUCTIONAL_VERBS)
	if verb_hits:
		score += min(2.0, 0.5 * verb_hits)
		reasons.append(f"verb_hits:{verb_hits}")
	if words:
		short = sum(1 for w in words if len(w) <= 3)
		ratio = short / max(1, len(words))
		if ratio > 0.55 and len(words) >= 12:
			score -= 2.0
			reasons.append("nav_like_short_word_ratio")
	if len(text) < 40:
		score -= 1.5
		reasons.append("too_short")
	is_instructional = score >= 0.5
	return Classification(is_instructional, score, tuple(reasons))

def filter_instructional_sections(
	sections: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Tuple[Dict[str, Any], Classification]]]:
	kept: List[Dict[str, Any]] = []
	dropped: List[Tuple[Dict[str, Any], Classification]] = []
	for sec in sections:
		cls = classify_section(sec)
		if cls.is_instructional:
			kept.append(sec)
		else:
			dropped.append((sec, cls))
	return kept, dropped

# Moved from ingestion/section_classifier.py
