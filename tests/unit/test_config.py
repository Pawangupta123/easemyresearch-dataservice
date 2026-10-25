import pytest
from pydantic import ValidationError

from dataservice.config import Settings
from tests.conftest import SECRET


def test_unsafe_rename_is_refused() -> None:
    with pytest.raises(ValidationError, match="UNSAFE_RENAME"):
        Settings(service_token_secret=SECRET, storage_mode="s3", aws_s3_allow_unsafe_rename=True)


def test_s3_starts_without_a_lock_table() -> None:
    # S3 conditional writes coordinate commits (consistency §3, C0) — no DynamoDB needed.
    assert Settings(service_token_secret=SECRET, storage_mode="s3").storage_mode == "s3"


def test_a_short_service_secret_is_refused() -> None:
    with pytest.raises(ValidationError, match="at least 32"):
        Settings(service_token_secret="short", storage_mode="local")


def test_a_retention_no_longer_than_the_restore_window_is_refused() -> None:
    with pytest.raises(ValidationError, match="must exceed RESTORE_WINDOW_DAYS"):
        Settings(service_token_secret=SECRET, storage_mode="local", delta_retention_days=30, restore_window_days=30)
    assert Settings(service_token_secret=SECRET, storage_mode="local").delta_retention_days == 60
