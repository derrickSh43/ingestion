from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import hashlib
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from uuid import uuid4
import html as _html
import zipfile
import xml.etree.ElementTree as ET
import io

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from cleaner import clean_html_text
from env import (
	get_canonical_root,
	get_chunks_root,
	get_embeddings_root,
	get_ingestion_data_root,
	get_observability_root,
	get_releases_root,
	get_vector_index_root,
)
from integrity import sign_content_hash
from observability import ObservabilityStore
from pipeline import run_ingestion
from releases import ReleaseManager
from retrieval_service import RetrievalService


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

class IngestionRunBatchItem(BaseModel):
	source_id: str
	raw_html: Optional[str] = None
	raw_html_path: Optional[str] = None
	capture_id: Optional[str] = None

class IngestionRunBatchRequest(BaseModel):
	domain: str
	release_id: Optional[str] = None
	created_by: Optional[str] = None
	continue_on_error: bool = False
	force: bool = False
	items: List[IngestionRunBatchItem]

class IngestionRunBatchResponse(BaseModel):
	domain: str
	release_id: str
	release: Dict[str, Any]
	summary: Dict[str, Any]
	results: List[Dict[str, Any]]


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

class IngestionRawCaptureBatchItem(BaseModel):
	source_id: str
	url: str
	timeout: int = 10
	persist_to_db: bool = False
	clean: bool = False
	quarantine_suspicious: bool = True

class IngestionRawCaptureBatchRequest(BaseModel):
	domain: str
	continue_on_error: bool = False
	items: List[IngestionRawCaptureBatchItem]

class IngestionRawCaptureBatchResponse(BaseModel):
	domain: str
	summary: Dict[str, int]
	results: List[Dict[str, Any]]


class IngestionIngestBatchItem(BaseModel):
	source_id: str
	url: str
	timeout: int = 10
	clean: bool = False
	quarantine_suspicious: bool = True

class IngestionIngestBatchRequest(BaseModel):
	domain: str
	release_id: Optional[str] = None
	created_by: Optional[str] = None
	continue_on_error: bool = False
	force: bool = False
	items: List[IngestionIngestBatchItem]

class IngestionIngestBatchResponse(BaseModel):
	domain: str
	release_id: str
	release: Dict[str, Any]
	summary: Dict[str, Any]
	results: List[Dict[str, Any]]


class IngestionQuarantineRequest(BaseModel):
	domain: str
	capture_id: str
	reason: Optional[str] = None


class ReleasePromoteRequest(BaseModel):
	reason: Optional[str] = None
	promoted_by: Optional[str] = None

class ReleaseMergeRequest(BaseModel):
	source_release_ids: List[str]
	target_release_id: Optional[str] = None
	created_by: Optional[str] = None

class ReleaseMergeResponse(BaseModel):
	domain: str
	target_release_id: str
	source_release_ids: List[str]
	summary: Dict[str, Any]
	release: Dict[str, Any]


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

def _safe_slug(value: str) -> str:
	raw = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in (value or "").strip().lower())
	raw = raw.strip("_")
	return raw or "domain"

def _generate_release_id(domain: str) -> str:
	ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
	suffix = str(uuid4()).split("-", 1)[0]
	return f"{_safe_slug(domain)}_{ts}_{suffix}"

def _iter_domain_dirs(root: Path) -> List[str]:
	if not root.exists():
		return []
	out: List[str] = []
	for p in root.iterdir():
		if p.is_dir():
			out.append(p.name)
	return out

@app.get("/domains")
def list_domains() -> Dict[str, Any]:
	data_root = get_ingestion_data_root()
	candidates: List[str] = []
	for d in (
		data_root / "captures",
		get_releases_root(),
		get_observability_root(),
		get_canonical_root(),
		get_chunks_root(),
		get_embeddings_root(),
		get_vector_index_root(),
	):
		candidates.extend(_iter_domain_dirs(Path(d)))
	unique = sorted({c for c in candidates if isinstance(c, str) and c.strip()})
	return {"domains": unique}


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

def _model_to_dict(value: Any) -> Any:
	if value is None:
		return None
	dumper = getattr(value, "model_dump", None)
	if callable(dumper):
		return dumper()
	to_dict = getattr(value, "dict", None)
	if callable(to_dict):
		return to_dict()
	return value


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
			write_release=True,
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

@app.post("/ingestion/run/batch", response_model=IngestionRunBatchResponse)
def ingestion_run_batch(req: IngestionRunBatchRequest) -> IngestionRunBatchResponse:
	if not req.domain or not req.domain.strip():
		raise HTTPException(status_code=400, detail="domain is required")
	if not req.items:
		raise HTTPException(status_code=400, detail="items is required")

	release_id = (req.release_id or "").strip() or _generate_release_id(req.domain)
	release_manager = ReleaseManager()
	release_meta = release_manager.create_release(
		domain=req.domain,
		release_id=release_id,
		created_by=req.created_by,
		payload={"mode": "batch"},
	)

	results: List[Dict[str, Any]] = []
	agg_counts: Dict[str, int] = {"sections_total": 0, "sections_kept": 0, "canonical_objects": 0, "chunks": 0, "embeddings": 0}
	succeeded = 0

	def _load_item_raw_html(item: IngestionRunBatchItem) -> str:
		if item.raw_html and item.raw_html.strip():
			return item.raw_html
		if item.raw_html_path:
			path = Path(item.raw_html_path).expanduser().resolve()
			if not path.exists():
				raise FileNotFoundError("raw_html_path not found")
			return path.read_text(encoding="utf-8")
		if item.capture_id:
			meta = _load_capture(req.domain, item.capture_id)
			path = Path(str(meta.get("raw_html_path", ""))).expanduser().resolve()
			if not path.exists():
				raise FileNotFoundError("capture raw_html_path not found")
			if not req.force:
				capture_ok = bool(meta.get("capture_ok"))
				quarantined = bool(meta.get("quarantined"))
				http_status = meta.get("http_status")
				if quarantined or not capture_ok:
					raise RuntimeError(f"capture not usable (http_status={http_status}, quarantined={quarantined})")
			return path.read_text(encoding="utf-8")
		raise RuntimeError("raw_html/raw_html_path/capture_id is required")

	for item in req.items:
		try:
			raw_html = _load_item_raw_html(item)
			run_result = run_ingestion(
				domain=req.domain,
				source_id=item.source_id,
				release_id=release_id,
				raw_html=raw_html,
				created_by=req.created_by,
				write_release=False,
			)
			succeeded += 1
			for k, v in (run_result.counts or {}).items():
				agg_counts[k] = int(agg_counts.get(k, 0)) + int(v)
			results.append({"source_id": item.source_id, "ok": True, "counts": run_result.counts})
			ObservabilityStore().record_event(
				domain=req.domain,
				event="ingestion_run_batch_item",
				status="success",
				release_id=release_id,
				source_id=item.source_id,
			)
		except Exception as exc:
			results.append({"source_id": item.source_id, "ok": False, "error": {"message": str(exc)}})
			ObservabilityStore().record_event(
				domain=req.domain,
				event="ingestion_run_batch_item",
				status="error",
				release_id=release_id,
				source_id=item.source_id,
				error=str(exc),
			)
			if not req.continue_on_error:
				raise HTTPException(
					status_code=400,
					detail={"message": "Batch ingestion failed", "release_id": release_id, "failed_source_id": item.source_id, "error": str(exc)},
				) from exc

	summary: Dict[str, Any] = {
		"total": len(req.items),
		"succeeded": succeeded,
		"failed": len(req.items) - succeeded,
		"counts": agg_counts,
	}
	status = "success" if succeeded == len(req.items) else ("failed" if succeeded == 0 else "partial")
	ObservabilityStore().record_event(
		domain=req.domain,
		event="ingestion_run_batch",
		status=status,
		release_id=release_id,
		total=len(req.items),
		succeeded=succeeded,
		failed=len(req.items) - succeeded,
	)
	return IngestionRunBatchResponse(domain=req.domain, release_id=release_id, release=release_meta, summary=summary, results=results)


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

@app.post("/ingestion/raw-capture/batch", response_model=IngestionRawCaptureBatchResponse)
def ingestion_raw_capture_batch(req: IngestionRawCaptureBatchRequest) -> IngestionRawCaptureBatchResponse:
	if not req.domain or not req.domain.strip():
		raise HTTPException(status_code=400, detail="domain is required")
	if not req.items:
		raise HTTPException(status_code=400, detail="items is required")

	results: List[Dict[str, Any]] = []
	counts = {"total": len(req.items), "capture_ok": 0, "failed": 0, "quarantined": 0}

	for item in req.items:
		try:
			r = ingestion_raw_capture(
				IngestionRawCaptureRequest(
					source_id=item.source_id,
					domain=req.domain,
					url=item.url,
					timeout=item.timeout,
					persist_to_db=item.persist_to_db,
					clean=item.clean,
					quarantine_suspicious=item.quarantine_suspicious,
				)
			)
			ok = bool(r.capture_ok)
			if ok:
				counts["capture_ok"] += 1
			else:
				counts["failed"] += 1
			if bool(r.quarantined):
				counts["quarantined"] += 1
			results.append({"source_id": item.source_id, "ok": ok, "capture": _model_to_dict(r)})
		except Exception as exc:
			counts["failed"] += 1
			results.append({"source_id": item.source_id, "ok": False, "error": {"message": str(exc)}})
			if not req.continue_on_error:
				raise HTTPException(
					status_code=400,
					detail={"message": "Batch capture failed", "failed_source_id": item.source_id, "error": str(exc)},
				) from exc

	return IngestionRawCaptureBatchResponse(domain=req.domain, summary=counts, results=results)

@app.post("/ingestion/ingest/batch", response_model=IngestionIngestBatchResponse)
def ingestion_ingest_batch(req: IngestionIngestBatchRequest) -> IngestionIngestBatchResponse:
	if not req.domain or not req.domain.strip():
		raise HTTPException(status_code=400, detail="domain is required")
	if not req.items:
		raise HTTPException(status_code=400, detail="items is required")

	release_id = (req.release_id or "").strip() or _generate_release_id(req.domain)
	release_manager = ReleaseManager()
	release_meta = release_manager.create_release(
		domain=req.domain,
		release_id=release_id,
		created_by=req.created_by,
		payload={"mode": "capture+run"},
	)

	results: List[Dict[str, Any]] = []
	agg_counts: Dict[str, int] = {"sections_total": 0, "sections_kept": 0, "canonical_objects": 0, "chunks": 0, "embeddings": 0}
	succeeded = 0

	for item in req.items:
		try:
			capture = ingestion_raw_capture(
				IngestionRawCaptureRequest(
					source_id=item.source_id,
					domain=req.domain,
					url=item.url,
					timeout=item.timeout,
					persist_to_db=False,
					clean=item.clean,
					quarantine_suspicious=item.quarantine_suspicious,
				)
			)
			if (not req.force) and (not capture.capture_ok or capture.quarantined):
				raise RuntimeError(f"capture not usable (http_status={capture.http_status}, quarantined={capture.quarantined})")
			raw_html = Path(capture.raw_html_path).read_text(encoding="utf-8")
			run_result = run_ingestion(
				domain=req.domain,
				source_id=item.source_id,
				release_id=release_id,
				raw_html=raw_html,
				created_by=req.created_by,
				write_release=False,
			)
			succeeded += 1
			for k, v in (run_result.counts or {}).items():
				agg_counts[k] = int(agg_counts.get(k, 0)) + int(v)
			results.append(
				{
					"source_id": item.source_id,
					"ok": True,
					"capture": _model_to_dict(capture),
					"counts": run_result.counts,
				}
			)
			ObservabilityStore().record_event(
				domain=req.domain,
				event="ingestion_ingest_batch_item",
				status="success",
				release_id=release_id,
				source_id=item.source_id,
			)
		except Exception as exc:
			results.append({"source_id": item.source_id, "ok": False, "error": {"message": str(exc)}})
			ObservabilityStore().record_event(
				domain=req.domain,
				event="ingestion_ingest_batch_item",
				status="error",
				release_id=release_id,
				source_id=item.source_id,
				error=str(exc),
			)
			if not req.continue_on_error:
				raise HTTPException(
					status_code=400,
					detail={"message": "Batch capture+ingest failed", "release_id": release_id, "failed_source_id": item.source_id, "error": str(exc)},
				) from exc

	summary: Dict[str, Any] = {
		"total": len(req.items),
		"succeeded": succeeded,
		"failed": len(req.items) - succeeded,
		"counts": agg_counts,
	}
	status = "success" if succeeded == len(req.items) else ("failed" if succeeded == 0 else "partial")
	ObservabilityStore().record_event(
		domain=req.domain,
		event="ingestion_ingest_batch",
		status=status,
		release_id=release_id,
		total=len(req.items),
		succeeded=succeeded,
		failed=len(req.items) - succeeded,
	)
	return IngestionIngestBatchResponse(domain=req.domain, release_id=release_id, release=release_meta, summary=summary, results=results)

@app.post("/ingestion/file-capture", response_model=IngestionRawCaptureResponse)
async def ingestion_file_capture(
	domain: str = Form(...),
	source_id: str = Form(...),
	file: UploadFile = File(...),
	clean: bool = Form(False),
	quarantine_suspicious: bool = Form(True),
) -> IngestionRawCaptureResponse:
	if not domain or not domain.strip():
		raise HTTPException(status_code=400, detail="domain is required")
	if not source_id or not source_id.strip():
		raise HTTPException(status_code=400, detail="source_id is required")
	if not file:
		raise HTTPException(status_code=400, detail="file is required")

	raw_bytes = await file.read()
	filename = (file.filename or "").strip()
	ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""

	def _wrap_text_as_html(text: str) -> str:
		escaped = _html.escape(text or "")
		return f"<pre>{escaped}</pre>"

	def _best_effort_doc_text(data: bytes) -> str:
		for enc in ("utf-16le", "utf-8", "latin-1"):
			try:
				t = data.decode(enc, errors="ignore")
				t = t.replace("\x00", "")
				if t.strip():
					return t
			except Exception:
				continue
		return ""

	def _extract_docx_text(data: bytes) -> str:
		with zipfile.ZipFile(io.BytesIO(data)) as zf:
			xml_bytes = zf.read("word/document.xml")
		root = ET.fromstring(xml_bytes)
		parts: List[str] = []
		for el in root.iter():
			if el.tag.endswith("}t") and el.text:
				parts.append(el.text)
		return "\n".join(parts).strip()

	raw_text = ""
	raw_html = ""
	parse_error: Optional[str] = None
	try:
		if ext in ("html", "htm"):
			raw_html = raw_bytes.decode("utf-8", errors="replace")
		elif ext in ("txt", "md"):
			raw_text = raw_bytes.decode("utf-8", errors="replace")
			raw_html = _wrap_text_as_html(raw_text)
		elif ext == "docx":
			raw_text = _extract_docx_text(raw_bytes)
			raw_html = _wrap_text_as_html(raw_text)
		elif ext == "doc":
			raw_text = _best_effort_doc_text(raw_bytes)
			raw_html = _wrap_text_as_html(raw_text)
		else:
			raw_text = raw_bytes.decode("utf-8", errors="replace")
			raw_html = _wrap_text_as_html(raw_text)
	except Exception as exc:
		raw_html = ""
		parse_error = str(exc)
		ObservabilityStore().record_event(
			domain=domain,
			event="ingestion_file_capture",
			status="error",
			source_id=source_id,
			filename=filename,
			error=f"Failed to parse file: {exc}",
		)

	capture_ok = bool(raw_html.strip())
	quarantined = bool(quarantine_suspicious and not capture_ok)
	if not quarantined:
		quarantine_reason = None
	elif parse_error:
		quarantine_reason = "file_parse_failed"
	else:
		quarantine_reason = "empty_file"

	root = _capture_root(domain)
	root.mkdir(parents=True, exist_ok=True)
	raw_path = _capture_html_path(domain, source_id)
	raw_path.write_text(raw_html, encoding="utf-8")

	content_hash = f"sha256:{_sha256_hex(raw_html)}"
	content_signature = sign_content_hash(content_hash)
	payload: Dict[str, Any] = {
		"source_id": source_id,
		"domain": domain,
		"url": None,
		"http_status": 200 if capture_ok else 400,
		"headers": {"filename": filename, "content_type": file.content_type, "ext": ext},
		"raw_html_path": str(raw_path),
		"content_hash": content_hash,
		"content_signature": content_signature,
		"retrieved_at": _utc_now_iso(),
		"capture_ok": capture_ok,
		"cleaned_text": clean_html_text(raw_html) if clean else None,
		"quarantined": quarantined,
		"quarantine_reason": quarantine_reason,
		"quarantined_at": None,
		"db_persisted": False,
		"db_error": None,
	}
	_save_capture(domain, source_id, payload)
	ObservabilityStore().record_event(
		domain=domain,
		event="ingestion_file_capture",
		status="success" if capture_ok else "failed",
		source_id=source_id,
		filename=file.filename,
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

def _is_file_embedding_ref(ref: str) -> bool:
	return isinstance(ref, str) and ref.startswith("file:") and bool(ref[len("file:") :])

def _copy_rewrite_json(src: Path, dest: Path, patcher) -> None:
	payload = json.loads(src.read_text(encoding="utf-8"))
	if not isinstance(payload, dict):
		raise ValueError("expected JSON object")
	patcher(payload)
	dest.parent.mkdir(parents=True, exist_ok=True)
	dest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def _merge_releases(
	*,
	domain: str,
	source_release_ids: List[str],
	target_release_id: str,
) -> Dict[str, Any]:
	canonical_root = get_canonical_root()
	chunks_root = get_chunks_root()
	embeddings_root = get_embeddings_root()
	vector_root = get_vector_index_root()

	target_canonical_dir = canonical_root / domain / target_release_id
	target_chunks_dir = chunks_root / domain / target_release_id
	target_embeddings_dir = embeddings_root / domain / target_release_id
	target_index_path = vector_root / domain / target_release_id / "index.jsonl"

	target_canonical_dir.mkdir(parents=True, exist_ok=True)
	target_chunks_dir.mkdir(parents=True, exist_ok=True)
	target_embeddings_dir.mkdir(parents=True, exist_ok=True)
	target_index_path.parent.mkdir(parents=True, exist_ok=True)

	merged_rows: Dict[str, Dict[str, Any]] = {}
	duplicates = 0

	# Copy canonical artifacts (best-effort; not required for retrieval but useful for consistency)
	for src_rid in source_release_ids:
		src_dir = canonical_root / domain / src_rid
		if not src_dir.exists():
			continue
		for p in src_dir.glob("*.json"):
			dest = target_canonical_dir / p.name
			if dest.exists():
				continue
			def _patch(obj: Dict[str, Any]) -> None:
				obj["domain"] = domain
				prov = obj.get("provenance")
				if isinstance(prov, dict):
					prov["release_id"] = target_release_id
			_copy_rewrite_json(p, dest, _patch)

	# Merge index rows and copy chunk + embedding artifacts
	for src_rid in source_release_ids:
		src_index_path = vector_root / domain / src_rid / "index.jsonl"
		if not src_index_path.exists():
			continue
		for line in src_index_path.read_text(encoding="utf-8").splitlines():
			line = line.strip()
			if not line:
				continue
			row = json.loads(line)
			if not isinstance(row, dict):
				continue
			chunk_id = str(row.get("chunk_id", "")).strip()
			if not chunk_id:
				continue
			if chunk_id in merged_rows:
				duplicates += 1
				continue

			src_chunk_path = chunks_root / domain / src_rid / f"{chunk_id}.json"
			if not src_chunk_path.exists():
				raise FileNotFoundError(f"missing chunk file for {chunk_id} in release {src_rid}")
			dest_chunk_path = target_chunks_dir / f"{chunk_id}.json"
			def _patch_chunk(obj: Dict[str, Any]) -> None:
				obj["domain"] = domain
				obj["release_id"] = target_release_id
			_copy_rewrite_json(src_chunk_path, dest_chunk_path, _patch_chunk)

			emb_ref = str(row.get("embedding_ref", "")).strip()
			if not _is_file_embedding_ref(emb_ref):
				raise ValueError(f"unsupported embedding_ref for {chunk_id}: {emb_ref}")
			src_emb_path = Path(emb_ref[len("file:") :]).expanduser().resolve()
			if not src_emb_path.exists():
				raise FileNotFoundError(f"missing embedding file for {chunk_id}: {src_emb_path}")
			dest_emb_path = target_embeddings_dir / src_emb_path.name
			def _patch_emb(obj: Dict[str, Any]) -> None:
				obj["domain"] = domain
				obj["release_id"] = target_release_id
			_copy_rewrite_json(src_emb_path, dest_emb_path, _patch_emb)

			new_row = dict(row)
			new_row["domain"] = domain
			new_row["release_id"] = target_release_id
			new_row["embedding_ref"] = f"file:{dest_emb_path.as_posix()}"
			merged_rows[chunk_id] = new_row

	# Deterministic write order
	lines = [json.dumps(merged_rows[k], ensure_ascii=False) for k in sorted(merged_rows.keys())]
	target_index_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
	return {"rows_written": len(lines), "duplicates_skipped": duplicates, "source_releases": len(source_release_ids)}


@app.post("/releases/{domain}/merge", response_model=ReleaseMergeResponse)
def merge_releases(domain: str, req: ReleaseMergeRequest) -> ReleaseMergeResponse:
	if not domain or not domain.strip():
		raise HTTPException(status_code=400, detail="domain is required")
	source_release_ids = [str(x).strip() for x in (req.source_release_ids or []) if str(x).strip()]
	if len(source_release_ids) < 2:
		raise HTTPException(status_code=400, detail="source_release_ids must include at least two releases")

	target_release_id = (req.target_release_id or "").strip() or _generate_release_id(domain)

	manager = ReleaseManager()
	release_meta = manager.create_release(
		domain=domain,
		release_id=target_release_id,
		created_by=req.created_by,
		payload={"mode": "merge", "source_release_ids": source_release_ids},
	)

	try:
		summary = _merge_releases(domain=domain, source_release_ids=source_release_ids, target_release_id=target_release_id)
		ObservabilityStore().record_event(
			domain=domain,
			event="release_merge",
			status="success",
			release_id=target_release_id,
			source_release_ids=source_release_ids,
			summary=summary,
		)
	except Exception as exc:
		ObservabilityStore().record_event(
			domain=domain,
			event="release_merge",
			status="error",
			release_id=target_release_id,
			source_release_ids=source_release_ids,
			error=str(exc),
		)
		raise HTTPException(status_code=400, detail={"message": "Release merge failed", "error": str(exc)}) from exc

	return ReleaseMergeResponse(
		domain=domain,
		target_release_id=target_release_id,
		source_release_ids=source_release_ids,
		summary=summary,
		release=release_meta,
	)


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
