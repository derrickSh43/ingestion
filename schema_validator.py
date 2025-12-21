import json
import re
from pathlib import Path
from typing import Any, Dict

from .env import BASE_DIR

SCHEMAS_DIR = BASE_DIR / "docs" / "schemas"

def load_schema(name: str) -> Dict[str, Any]:
	path = SCHEMAS_DIR / name
	with open(path, "r", encoding="utf-8") as fh:
		return json.load(fh)

try:
	import jsonschema  # type: ignore
	from jsonschema import Draft7Validator  # type: ignore

	def validate_instance(schema_name: str, instance: Dict[str, Any]) -> None:
		schema = load_schema(schema_name)
		validator = Draft7Validator(schema)
		errors = sorted(validator.iter_errors(instance), key=lambda e: e.path)
		if errors:
			msgs = [f"{list(e.path)}: {e.message}" for e in errors]
			raise ValueError("Schema validation errors: " + "; ".join(msgs))

except ModuleNotFoundError:
	# Minimal fallback validator when jsonschema is not installed.
	def validate_instance(schema_name: str, instance: Dict[str, Any]) -> None:
		schema = load_schema(schema_name)
		# Check required fields
		req = schema.get("required", [])
		missing = [r for r in req if r not in instance]
		if missing:
			raise ValueError(f"Missing required fields: {missing}")

		# Quick pattern checks for fields we care about in tests
		props = schema.get("properties", {})
		chash = props.get("content_hash", {})
		pattern = chash.get("pattern") if isinstance(chash, dict) else None
		if pattern and "content_hash" in instance:
			if not re.match(pattern, instance["content_hash"]):
				raise ValueError("content_hash does not match required pattern")

if __name__ == "__main__":
	ex = {
		"source_id": "src_terraform_001",
		"domain": "terraform",
		"type": "url",
		"url": "https://example.com",
		"submitted_at": "2025-12-17T10:30:00Z",
	}
	try:
		validate_instance("content_source.json", ex)
		print("OK")
	except Exception as e:
		print("Validation failed:", e)
