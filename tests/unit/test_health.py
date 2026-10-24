import time

from fastapi.testclient import TestClient

from dataservice.common.utils import service_token
from dataservice.main import app
from tests.conftest import SECRET

client = TestClient(app)


def test_health_with_a_valid_token() -> None:
    token = service_token.sign(SECRET, int(time.time()))
    assert client.get("/health", headers={"x-service-token": token}).json() == {"status": "ok"}


def test_health_rejected_without_a_token() -> None:
    assert client.get("/health").status_code == 401


def test_health_rejected_with_an_expired_or_forged_token() -> None:
    expired = service_token.sign(SECRET, int(time.time()) - 3600)
    forged = service_token.sign("another-secret-that-is-32-characters-long", int(time.time()))
    assert client.get("/health", headers={"x-service-token": expired}).status_code == 401
    assert client.get("/health", headers={"x-service-token": forged}).status_code == 401
