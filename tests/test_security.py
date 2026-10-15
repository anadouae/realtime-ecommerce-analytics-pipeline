import logging
import os

from pipeline.security import (
    RedactingFilter,
    configure_secure_logging,
    kafka_security_kwargs,
    redact_secrets,
    safe_repr_mapping,
)
from pipeline.sinks.snowflake_stub import SnowflakeAdapter


def test_redact_password_kv():
    assert "***REDACTED***" in redact_secrets("password=super-secret-value")
    assert "super-secret-value" not in redact_secrets("password=super-secret-value")


def test_redact_env_leak(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "leaked-pw-xyz")
    assert "leaked-pw-xyz" not in redact_secrets("connect failed leaked-pw-xyz")


def test_safe_repr_mapping():
    out = safe_repr_mapping({"user": "alice", "password": "secret"})
    assert out["user"] == "alice"
    assert out["password"] == "***REDACTED***"


def test_snowflake_repr_redacts_password():
    adapter = SnowflakeAdapter(account="xy123", user="u", password="hunter2")
    text = repr(adapter)
    assert "hunter2" not in text
    assert "***REDACTED***" in text


def test_kafka_security_kwargs_empty_by_default(monkeypatch):
    for key in (
        "KAFKA_SECURITY_PROTOCOL",
        "KAFKA_SASL_MECHANISM",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)
    assert kafka_security_kwargs() == {}


def test_kafka_security_kwargs_sasl(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "alice")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pw")
    kwargs = kafka_security_kwargs()
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_plain_username"] == "alice"
    assert kwargs["sasl_plain_password"] == "pw"


def test_redacting_filter_on_logger(caplog):
    configure_secure_logging(logging.INFO)
    logger = logging.getLogger("pipeline.security.test")
    with caplog.at_level(logging.INFO):
        logger.info("auth password=should-not-appear")
    assert "should-not-appear" not in caplog.text
    assert "***REDACTED***" in caplog.text or "password=" in caplog.text
