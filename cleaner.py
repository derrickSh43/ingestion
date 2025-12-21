"""Deterministic content cleaner.

Provides functions to clean raw HTML into normalized plain text for downstream
stages. Implemented without heavy external dependencies to remain CI-friendly.

Functions:
- clean_html_text(html_text: str) -> str
- clean_html_file(path: Path) -> str
"""
from __future__ import annotations

from pathlib import Path
import re
import html
from typing import Optional

# Patterns to remove script/style blocks
_RE_SCRIPT = re.compile(r"<script[\s\S]*?<\/script>", flags=re.IGNORECASE)
_RE_STYLE = re.compile(r"<style[\s\S]*?<\/style>", flags=re.IGNORECASE)
# Strip all tags
_RE_TAGS = re.compile(r"<[^>]+>")
# Collapse whitespace
_RE_WS = re.compile(r"\s+")
# Remove space before punctuation (e.g. turn "word !" into "word!")
_RE_SPACE_BEFORE_PUNC = re.compile(r"\s+([\.,!\?:;])")

def clean_html_text(html_text: Optional[str]) -> str:
	if not html_text:
		return ""
	t = html_text
	t = _RE_SCRIPT.sub(" ", t)
	t = _RE_STYLE.sub(" ", t)
	t = _RE_TAGS.sub(" ", t)
	t = html.unescape(t)
	t = _RE_WS.sub(" ", t)
	t = _RE_SPACE_BEFORE_PUNC.sub(r"\1", t)
	return t.strip()

def clean_html_file(path: Path) -> str:
	p = Path(path)
	if not p.exists():
		raise FileNotFoundError(str(p))
	raw = p.read_text(encoding="utf-8")
	return clean_html_text(raw)
# Moved from ingestion/cleaner.py
