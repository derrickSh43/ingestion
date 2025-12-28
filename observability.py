"""
ObservabilityStore: Monitoring & audit for ingestion/retrieval/release operations.
- Append-only JSONL event log per domain
- Cheap counters and on-demand summaries
- Admin-only API endpoints are implemented in main.py

This avoids adding heavy dependencies (Prometheus/OpenTelemetry) while still
providing useful operational visibility and a stable contract for future work.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import json

from env import get_observability_root as _get_observability_root

def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

def _parse_iso_z(ts: str) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    raw = ts.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def get_observability_root() -> Path:
    return _get_observability_root()

@dataclass
class ObservabilityStore:
    root: Path = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.root is None:
            self.root = get_observability_root()

    def _domain_dir(self, domain: str) -> Path:
        if not domain:
            raise ValueError("domain is required")
        return self.root / domain

    def _events_path(self, domain: str) -> Path:
        return self._domain_dir(domain) / "events.jsonl"

    def _counters_path(self, domain: str) -> Path:
        return self._domain_dir(domain) / "counters.json"

    def record_event(self, *, domain: str, event: str, status: str = "success", level: str = "INFO", **fields: Any) -> Dict[str, Any]:
        dd = self._domain_dir(domain)
        dd.mkdir(parents=True, exist_ok=True)

        payload: Dict[str, Any] = {
            "timestamp": _utc_now_iso(),
            "domain": domain,
            "event": str(event),
            "status": str(status),
            "level": str(level),
        }
        payload.update(fields)

        with self._events_path(domain).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

        # Update simple counters
        self.increment(domain=domain, key=f"event:{event}")
        self.increment(domain=domain, key=f"status:{status}")
        self.increment(domain=domain, key=f"event_status:{event}:{status}")

        return payload

    def increment(self, *, domain: str, key: str, amount: int = 1) -> None:
        dd = self._domain_dir(domain)
        dd.mkdir(parents=True, exist_ok=True)
        path = self._counters_path(domain)

        counters: Dict[str, int] = {}
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        if isinstance(k, str) and isinstance(v, int):
                            counters[k] = v
            except Exception:
                counters = {}

        counters[key] = int(counters.get(key, 0)) + int(amount)
        path.write_text(json.dumps(counters, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")

    def list_events(self, *, domain: str, limit: int = 100) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        p = self._events_path(domain)
        if not p.exists():
            return []
        lines = p.read_text(encoding="utf-8").splitlines()
        # last N, newest first
        out: List[Dict[str, Any]] = []
        for line in reversed(lines[-limit:]):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
        return out

    def summarize(self, *, domain: str, hours: int = 24) -> Dict[str, Any]:
        now = datetime.now(tz=timezone.utc)
        since = now
        if hours is not None and int(hours) > 0:
            since = now - timedelta(hours=int(hours))  # type: ignore[name-defined]

        events = self.list_events(domain=domain, limit=10_000)
        filtered: List[Dict[str, Any]] = []
        for e in events:
            ts = _parse_iso_z(str(e.get("timestamp", "")))
            if ts is None:
                continue
            if ts >= since:
                filtered.append(e)

        counts_by_event: Dict[str, int] = {}
        counts_by_status: Dict[str, int] = {}
        for e in filtered:
            ev = str(e.get("event", ""))
            st = str(e.get("status", ""))
            counts_by_event[ev] = counts_by_event.get(ev, 0) + 1
            counts_by_status[st] = counts_by_status.get(st, 0) + 1

        integrity_failures = 0
        for e in filtered:
            if str(e.get("event")) in ("ingestion_integrity_failure", "ingestion_quarantine") or str(e.get("status")) in ("error", "failed"):
                if str(e.get("event")) == "ingestion_integrity_failure":
                    integrity_failures += 1

        alerts: List[Dict[str, Any]] = []
        if integrity_failures > 0:
            alerts.append({"type": "integrity_failure", "count": integrity_failures, "severity": "high"})
        quarantined = counts_by_event.get("ingestion_quarantine", 0)
        if quarantined > 0:
            alerts.append({"type": "quarantine", "count": quarantined, "severity": "medium"})

        return {
            "domain": domain,
            "window_hours": int(hours),
            "event_count": len(filtered),
            "counts_by_event": counts_by_event,
            "counts_by_status": counts_by_status,
            "alerts": alerts,
        }


# Python <3.11 compatibility for timedelta without extra imports in type checking
from datetime import timedelta  # noqa: E402
