"""Secrets hygiene: env-driven credentials and log redaction."""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Mapping, Optional

# Keys whose values must never appear in logs or reprs.
SENSITIVE_ENV_KEYS = (
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_PRIVATE_KEY",
    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    "KAFKA_SASL_PASSWORD",
    "POSTGRES_PASSWORD",
    "AWS_SECRET_ACCESS_KEY",
)

_REDACTED = "***REDACTED***"

_KV_SECRET = re.compile(
    r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|authorization|"
    r"private[_-]?key|sasl[_-]?password|snowflake[_-]?password)\b"
    r"(\s*[=:]\s*)([^\s,;\"']+)"
)
_BEARER = re.compile(r"(?i)(Bearer\s+)\S+")


def redact_secrets(text: str) -> str:
    """Return ``text`` with known secret patterns replaced."""
    if not text:
        return text
    out = _KV_SECRET.sub(rf"\1\2{_REDACTED}", text)
    out = _BEARER.sub(rf"\1{_REDACTED}", out)
    for key in SENSITIVE_ENV_KEYS:
        val = os.getenv(key)
        if val and len(val) >= 4 and val in out:
            out = out.replace(val, _REDACTED)
    return out


def safe_repr_mapping(
    data: Mapping[str, Any],
    sensitive_keys: Optional[set[str]] = None,
) -> dict[str, Any]:
    """Copy a mapping with sensitive keys redacted (case-insensitive name match)."""
    sensitive = sensitive_keys or {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "private_key",
        "sasl_password",
        "snowflake_password",
    }
    out: dict[str, Any] = {}
    for k, v in data.items():
        key_l = str(k).lower()
        if key_l in sensitive or any(
            s in key_l for s in ("password", "secret", "token", "private_key")
        ):
            out[k] = _REDACTED if v else v
        else:
            out[k] = v
    return out


class RedactingFilter(logging.Filter):
    """Logging filter that redacts secrets from record messages and args."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, str):
                record.msg = redact_secrets(record.msg)
            if record.args:
                if isinstance(record.args, dict):
                    record.args = {
                        k: redact_secrets(v) if isinstance(v, str) else v
                        for k, v in record.args.items()
                    }
                elif isinstance(record.args, tuple):
                    record.args = tuple(
                        redact_secrets(a) if isinstance(a, str) else a for a in record.args
                    )
        except Exception:  # noqa: BLE001 — never break logging
            pass
        return True


def configure_secure_logging(level: int = logging.INFO) -> None:
    """Install a redacting filter on the root logger (idempotent)."""
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
    else:
        root.setLevel(level)
    already = any(isinstance(f, RedactingFilter) for f in root.filters) or any(
        isinstance(f, RedactingFilter) for h in root.handlers for f in h.filters
    )
    if already:
        return
    filt = RedactingFilter()
    root.addFilter(filt)
    for handler in root.handlers:
        handler.addFilter(filt)


def kafka_security_kwargs() -> dict[str, Any]:
    """Optional Kafka SASL/SSL settings from environment (empty when unset).

    Placeholders documented in ``.env.example``:
      KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM,
      KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD
    """
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "").strip()
    if not protocol or protocol.upper() == "PLAINTEXT":
        return {}
    kwargs: dict[str, Any] = {"security_protocol": protocol}
    mechanism = os.getenv("KAFKA_SASL_MECHANISM", "").strip()
    if mechanism:
        kwargs["sasl_mechanism"] = mechanism
    username = os.getenv("KAFKA_SASL_USERNAME", "").strip()
    password = os.getenv("KAFKA_SASL_PASSWORD", "")
    if username:
        kwargs["sasl_plain_username"] = username
    if password:
        kwargs["sasl_plain_password"] = password
    return kwargs
