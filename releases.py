"""Release management & promotion (Step 10).

Goals:
- Each ingest run produces a release (immutable artifacts already live under
  data/{canonical,chunks,embeddings,vector_index}/<domain>/<release_id>/...).
- A domain has an *active* release, switched by writing active_release.txt.
- Promotion requires privileged role at the API layer (handled in main.py).
- Promotions are auditable via structured logs + a local audit.jsonl.

Storage layout:
  <root>/<domain>/
    active_release.txt
    releases/<release_id>/release.json
    audit.jsonl

Environment override:
  RELEASES_ROOT=/path/to/releases
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List
import json

from .env import get_releases_root as _get_releases_root


def _utc_now_iso() -> str:
	return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def get_releases_root() -> Path:
	return _get_releases_root()


@dataclass
class ReleaseManager:
	root: Path = None  # type: ignore[assignment]

	def __post_init__(self) -> None:
		if self.root is None:
			self.root = get_releases_root()

	def _domain_dir(self, domain: str) -> Path:
		if not domain:
			raise ValueError("domain is required")
		return self.root / domain

	def _release_dir(self, domain: str, release_id: str) -> Path:
		if not release_id:
			raise ValueError("release_id is required")
		return self._domain_dir(domain) / "releases" / release_id

	def _release_json_path(self, domain: str, release_id: str) -> Path:
		return self._release_dir(domain, release_id) / "release.json"

	def _active_release_path(self, domain: str) -> Path:
		return self._domain_dir(domain) / "active_release.txt"

	def _audit_path(self, domain: str) -> Path:
		return self._domain_dir(domain) / "audit.jsonl"

	def create_release(
		self,
		*,
		domain: str,
		release_id: str,
		created_by: str | None = None,
		payload: Optional[Dict[str, Any]] = None,
	) -> Dict[str, Any]:
		"""Create a release directory with minimal metadata."""
		meta = {
			"release_id": release_id,
			"domain": domain,
			"created_by": created_by,
			"created_at": _utc_now_iso(),
		}
		if payload and isinstance(payload, dict):
			meta.update(payload)

		release_dir = self._release_dir(domain, release_id)
		release_dir.mkdir(parents=True, exist_ok=True)
		self._domain_dir(domain).mkdir(parents=True, exist_ok=True)
		self._release_json_path(domain, release_id).write_text(
			json.dumps(meta, ensure_ascii=False, indent=2),
			encoding="utf-8",
		)
		return meta

	def get_active_release(self, domain: str) -> Optional[str]:
		path = self._active_release_path(domain)
		if not path.exists():
			return None
		raw = path.read_text(encoding="utf-8").strip()
		return raw or None

	def promote_release(
		self,
		*,
		domain: str,
		release_id: str,
		promoted_by: str | None = None,
		reason: str | None = None,
	) -> Dict[str, Any]:
		"""Mark release as active and append an audit entry."""
		previous = self.get_active_release(domain)
		self._domain_dir(domain).mkdir(parents=True, exist_ok=True)
		self._release_dir(domain, release_id).mkdir(parents=True, exist_ok=True)
		self._active_release_path(domain).write_text(release_id, encoding="utf-8")

		event = {
			"timestamp": _utc_now_iso(),
			"event": "security_release_promoted",
			"domain": domain,
			"release_id": release_id,
			"previous_release_id": previous,
			"actor": promoted_by,
			"reason": reason,
		}
		audit_path = self._audit_path(domain)
		audit_path.parent.mkdir(parents=True, exist_ok=True)
		with audit_path.open("a", encoding="utf-8") as fh:
			fh.write(json.dumps(event, ensure_ascii=False) + "\n")
		return event

	def list_audit(self, *, domain: str, limit: int = 100) -> List[Dict[str, Any]]:
		"""Return latest audit events (newest first)."""
		path = self._audit_path(domain)
		if not path.exists() or limit <= 0:
			return []
		lines = path.read_text(encoding="utf-8").splitlines()
		events: List[Dict[str, Any]] = []
		for line in reversed(lines[-limit:]):
			line = line.strip()
			if not line:
				continue
			try:
				obj = json.loads(line)
				if isinstance(obj, dict):
					events.append(obj)
			except Exception:
				continue
		return events
