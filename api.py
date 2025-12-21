from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import hashlib
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .cleaner import clean_html_text
from .env import get_ingestion_data_root
from .integrity import sign_content_hash
from .observability import ObservabilityStore
from .pipeline import run_ingestion
from .releases import ReleaseManager
from .retrieval_service import RetrievalService


app = FastAPI(title="Ingestion Service")
app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_methods=["*"],
	allow_headers=["*"],
)


class IngestionRunRequest(BaseModel):
	domain: str
	source_id: str
	release_id: str
	raw_html: Optional[str] = None
	raw_html_path: Optional[str] = None
	capture_id: Optional[str] = None
	created_by: Optional[str] = None


class IngestionRunResponse(BaseModel):
	status: str
	domain: str
	release_id: str
	release: Dict[str, Any]
	counts: Dict[str, int]


class IngestionRawCaptureRequest(BaseModel):
	source_id: str
	domain: str
	url: str
	timeout: int = 10
	persist_to_db: bool = True
	clean: bool = False
	quarantine_suspicious: bool = True


class IngestionRawCaptureResponse(BaseModel):
	source_id: str
	domain: Optional[str] = None
	url: Optional[str] = None
	http_status: int
	headers: Dict[str, Any]
	raw_html_path: str
	content_hash: str
	content_signature: str
	retrieved_at: str
	capture_ok: bool
	cleaned_text: Optional[str] = None
	quarantined: bool
	quarantine_reason: Optional[str] = None
	quarantined_at: Optional[str] = None
	db_persisted: bool
	db_error: Optional[str] = None


class IngestionQuarantineRequest(BaseModel):
	domain: str
	capture_id: str
	reason: Optional[str] = None


class ReleasePromoteRequest(BaseModel):
	reason: Optional[str] = None
	promoted_by: Optional[str] = None


class RetrieveRequest(BaseModel):
	domain: str
	query: str
	top_k: int = Field(default=5, ge=1, le=50)
	filters: Optional[Dict[str, Any]] = None
	release_id: Optional[str] = None


def _utc_now_iso() -> str:
	return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_hex(text: str) -> str:
	h = hashlib.sha256()
	h.update((text or "").encode("utf-8"))
	return h.hexdigest()


def _capture_root(domain: str) -> Path:
	return get_ingestion_data_root() / "captures" / domain


def _capture_html_path(domain: str, capture_id: str) -> Path:
	return _capture_root(domain) / f"{capture_id}.html"


def _capture_meta_path(domain: str, capture_id: str) -> Path:
	return _capture_root(domain) / f"{capture_id}.json"


def _load_capture(domain: str, capture_id: str) -> Dict[str, Any]:
	meta_path = _capture_meta_path(domain, capture_id)
	if not meta_path.exists():
		raise FileNotFoundError("capture not found")
	return json.loads(meta_path.read_text(encoding="utf-8"))


def _save_capture(domain: str, capture_id: str, payload: Dict[str, Any]) -> None:
	root = _capture_root(domain)
	root.mkdir(parents=True, exist_ok=True)
	_capture_meta_path(domain, capture_id).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _fetch_url(url: str, timeout: int) -> tuple[int, Dict[str, Any], str]:
	req = urllib.request.Request(url=url, headers={"User-Agent": "ingestion-service/1.0"})
	try:
		with urllib.request.urlopen(req, timeout=timeout) as resp:
			status = int(getattr(resp, "status", 200))
			raw = resp.read().decode("utf-8", errors="replace")
			headers = {k: v for k, v in resp.headers.items()}
			return status, headers, raw
	except urllib.error.HTTPError as exc:
		raw = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
		headers = dict(getattr(exc, "headers", {}) or {})
		return int(exc.code or 500), headers, raw
	except Exception as exc:
		raise RuntimeError(f"Failed to fetch URL: {exc}") from exc


@app.post("/ingestion/run", response_model=IngestionRunResponse)
def ingestion_run(req: IngestionRunRequest) -> IngestionRunResponse:
	raw_html = req.raw_html
	if not raw_html and req.raw_html_path:
		path = Path(req.raw_html_path).expanduser().resolve()
		if not path.exists():
			raise HTTPException(status_code=404, detail="raw_html_path not found")
		raw_html = path.read_text(encoding="utf-8")
	if not raw_html and req.capture_id:
		try:
			meta = _load_capture(req.domain, req.capture_id)
			path = Path(str(meta.get("raw_html_path", ""))).expanduser().resolve()
			if not path.exists():
				raise HTTPException(status_code=404, detail="capture raw_html_path not found")
			raw_html = path.read_text(encoding="utf-8")
		except FileNotFoundError:
			raise HTTPException(status_code=404, detail="capture not found")
	if not raw_html:
		raise HTTPException(status_code=400, detail="raw_html or raw_html_path is required")
	try:
		result = run_ingestion(
			domain=req.domain,
			source_id=req.source_id,
			release_id=req.release_id,
			raw_html=raw_html,
			created_by=req.created_by,
		)
		ObservabilityStore().record_event(
			domain=req.domain,
			event="ingestion_run",
			status="success",
			release_id=req.release_id,
			source_id=req.source_id,
		)
	except Exception as exc:
		ObservabilityStore().record_event(
			domain=req.domain,
			event="ingestion_run",
			status="error",
			release_id=req.release_id,
			source_id=req.source_id,
			error=str(exc),
		)
		raise HTTPException(status_code=400, detail=str(exc)) from exc
	return IngestionRunResponse(**result.__dict__)


@app.post("/ingestion/raw-capture", response_model=IngestionRawCaptureResponse)
def ingestion_raw_capture(req: IngestionRawCaptureRequest) -> IngestionRawCaptureResponse:
	if not req.domain or not req.domain.strip():
		raise HTTPException(status_code=400, detail="domain is required")
	if not req.source_id or not req.source_id.strip():
		raise HTTPException(status_code=400, detail="source_id is required")
	if not req.url or not req.url.strip():
		raise HTTPException(status_code=400, detail="url is required")
	try:
		status, headers, raw = _fetch_url(req.url, timeout=int(req.timeout or 10))
	except Exception as exc:
		ObservabilityStore().record_event(
			domain=req.domain,
			event="ingestion_raw_capture",
			status="error",
			source_id=req.source_id,
			url=req.url,
			error=str(exc),
		)
		raise HTTPException(status_code=400, detail=str(exc)) from exc

	capture_ok = 200 <= status < 300 and bool(raw.strip())
	quarantined = bool(req.quarantine_suspicious and not capture_ok)
	quarantine_reason = None if not quarantined else "capture_failed"

	root = _capture_root(req.domain)
	root.mkdir(parents=True, exist_ok=True)
	raw_path = _capture_html_path(req.domain, req.source_id)
	raw_path.write_text(raw, encoding="utf-8")

	content_hash = f"sha256:{_sha256_hex(raw)}"
	content_signature = sign_content_hash(content_hash)
	payload: Dict[str, Any] = {
		"source_id": req.source_id,
		"domain": req.domain,
		"url": req.url,
		"http_status": status,
		"headers": headers,
		"raw_html_path": str(raw_path),
		"content_hash": content_hash,
		"content_signature": content_signature,
		"retrieved_at": _utc_now_iso(),
		"capture_ok": capture_ok,
		"cleaned_text": clean_html_text(raw) if req.clean else None,
		"quarantined": quarantined,
		"quarantine_reason": quarantine_reason,
		"quarantined_at": None,
		"db_persisted": False,
		"db_error": None,
	}
	_save_capture(req.domain, req.source_id, payload)
	ObservabilityStore().record_event(
		domain=req.domain,
		event="ingestion_raw_capture",
		status="success" if capture_ok else "failed",
		source_id=req.source_id,
		url=req.url,
		http_status=status,
		quarantined=quarantined,
	)
	return IngestionRawCaptureResponse(**payload)


@app.post("/ingestion/quarantine", response_model=IngestionRawCaptureResponse)
def ingestion_quarantine(req: IngestionQuarantineRequest) -> IngestionRawCaptureResponse:
	try:
		payload = _load_capture(req.domain, req.capture_id)
	except FileNotFoundError:
		raise HTTPException(status_code=404, detail="capture not found")
	payload["quarantined"] = True
	payload["quarantine_reason"] = req.reason or "manual_quarantine"
	payload["quarantined_at"] = _utc_now_iso()
	_save_capture(req.domain, req.capture_id, payload)
	ObservabilityStore().record_event(
		domain=req.domain,
		event="ingestion_quarantine",
		status="success",
		source_id=req.capture_id,
		reason=payload["quarantine_reason"],
	)
	return IngestionRawCaptureResponse(**payload)


@app.get("/ingestion/{domain}/events")
def ingestion_events(domain: str, limit: int = 100) -> Dict[str, Any]:
	store = ObservabilityStore()
	return {"domain": domain, "events": store.list_events(domain=domain, limit=limit)}


@app.get("/ingestion/{domain}/metrics")
def ingestion_metrics(domain: str, hours: int = 24) -> Dict[str, Any]:
	store = ObservabilityStore()
	return store.summarize(domain=domain, hours=hours)


@app.get("/releases/{domain}")
def list_releases(domain: str) -> Dict[str, Any]:
	manager = ReleaseManager()
	active = manager.get_active_release(domain)
	releases_dir = manager.root / domain / "releases"
	release_ids = []
	if releases_dir.exists():
		release_ids = sorted([p.name for p in releases_dir.iterdir() if p.is_dir()])
	return {"domain": domain, "active_release": active, "releases": release_ids}


@app.get("/releases/{domain}/audit")
def release_audit(domain: str, limit: int = 100) -> Dict[str, Any]:
	manager = ReleaseManager()
	return {"domain": domain, "events": manager.list_audit(domain=domain, limit=limit)}


@app.post("/releases/{domain}/promote")
def promote_release_compat(domain: str, body: Dict[str, Any]) -> Dict[str, Any]:
	release_id = str(body.get("release_id") or "").strip()
	if not release_id:
		raise HTTPException(status_code=400, detail="release_id is required")
	manager = ReleaseManager()
	try:
		event = manager.promote_release(
			domain=domain,
			release_id=release_id,
			promoted_by=body.get("promoted_by"),
			reason=body.get("reason"),
		)
		ObservabilityStore().record_event(
			domain=domain,
			event="release_promoted",
			status="success",
			release_id=release_id,
			previous_release_id=event.get("previous_release_id"),
		)
		return {
			"domain": domain,
			"active_release_id": release_id,
			"previous_release_id": event.get("previous_release_id"),
			"audit_event": event,
		}
	except Exception as exc:
		raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/releases/{domain}/{release_id}/promote")
def promote_release(domain: str, release_id: str, req: ReleasePromoteRequest) -> Dict[str, Any]:
	manager = ReleaseManager()
	try:
		event = manager.promote_release(
			domain=domain,
			release_id=release_id,
			promoted_by=req.promoted_by,
			reason=req.reason,
		)
		ObservabilityStore().record_event(
			domain=domain,
			event="release_promoted",
			status="success",
			release_id=release_id,
			previous_release_id=event.get("previous_release_id"),
		)
		return event
	except Exception as exc:
		raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/retrieve")
def retrieve(req: RetrieveRequest) -> Dict[str, Any]:
	service = RetrievalService.from_env()
	try:
		return service.query(
			domain=req.domain,
			query=req.query,
			filters=req.filters,
			top_k=req.top_k,
			release_id=req.release_id,
		)
	except FileNotFoundError as exc:
		raise HTTPException(status_code=404, detail=str(exc)) from exc
	except Exception as exc:
		raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/retrieval/query")
def retrieve_compat(req: RetrieveRequest) -> Dict[str, Any]:
	return retrieve(req)
