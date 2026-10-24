import pytest
from pydantic import ValidationError

from dataservice.config import Settings
from tests.conftest import SECRET


def test_unsafe_rename_is_refused() -> None:
    with pytest.raises(ValidationError, match="UNSAFE_RENAME"):
        Settings(service_token_secret=SECRET, storage_mode="local", aws_s3_allow_unsafe_rename=True)


def test_s3_without_the_dynamodb_lock_is_refused() -> None:
    with pytest.raises(ValidationError, match="dynamodb"):
        Settings(service_token_secret=SECRET, storage_mode="s3")


def test_s3_with_the_dynamodb_lock_starts() -> None:
    settings = Settings(
        service_token_secret=SECRET,
        storage_mode="s3",
        aws_s3_locking_provider="dynamodb",
        delta_dynamo_table_name="delta_log",
    )
    assert settings.delta_dynamo_table_name == "delta_log"
