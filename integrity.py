"""Integrity helpers for ingestion artifacts (Step 12).

Provides signed checksum generation/verification for stored content.

Signing scheme:
- HMAC-SHA256 over the *content hash string* (e.g., "sha256:<hex>")
- Secret from env: INGESTION_SIGNING_SECRET

The signature is represented as: "hmac-sha256:<hex>"

This is not intended to replace a full KMS-based signing pipeline; it's a
lightweight, deterministic integrity check for v1 and for unit tests.
"""

from __future__ import annotations

import hmac
import hashlib
import os
from typing import Optional

def _get_secret() -> str:
	secret = os.environ.get("INGESTION_SIGNING_SECRET")
	if secret and str(secret).strip():
		return str(secret)

	# Dev fallback: keep local runs working, but make it noisy.
	import warnings

	warnings.warn(
		"INGESTION_SIGNING_SECRET not set; using insecure dev default. "
		"Set INGESTION_SIGNING_SECRET in production.",
		UserWarning,
	)
	return "dev-ingestion-signing-secret-CHANGE-IN-PRODUCTION"

def sign_content_hash(content_hash: str, *, secret: Optional[str] = None) -> str:
	"""Return an HMAC signature over `content_hash`."""
	sec = (secret if secret is not None else _get_secret()).encode("utf-8")
	msg = (content_hash or "").encode("utf-8")
	mac = hmac.new(sec, msg, digestmod=hashlib.sha256).hexdigest()
	return f"hmac-sha256:{mac}"

def verify_content_hash(content_hash: str, signature: str, *, secret: Optional[str] = None) -> bool:
	if not signature or not isinstance(signature, str):
		return False
	expected = sign_content_hash(content_hash, secret=secret)
	return hmac.compare_digest(expected, signature)
