from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment configuration — every tunable lives here, nothing is hardcoded elsewhere.

    Refuses to start in a configuration that breaks the consistency protocol (consistency.md
    §3: S3 commits are coordinated by S3 conditional writes — never by unsafe renames).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Shared secret Node signs short-lived service tokens with (plan step 0.5).
    service_token_secret: str = Field(min_length=32)
    service_token_max_age_seconds: int = 300

    log_level: str = "info"
    log_json: bool = True

    # Delta on S3. `local` is only for unit tests on the filesystem.
    storage_mode: str = Field(default="s3", pattern="^(s3|local)$")
    aws_s3_allow_unsafe_rename: bool = False

    # The DuckDB engine every read shares (storage/reader/engine.py). Unset = DuckDB's default
    # (all cores, 80% of RAM). Size them to the host: reads beyond the memory limit spill to
    # the temp directory instead of failing.
    duckdb_threads: int | None = Field(default=None, ge=1)
    duckdb_memory_limit: str | None = Field(default=None, pattern=r"^\d+(\.\d+)?\s?(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$")
    duckdb_temp_directory: str | None = None

    # Old Delta versions (consistency.md §11): tables keep their log and removed files this long
    # (fenced_commit.retention_configuration); it must outlast the longest pin window — an
    # explicit dataset restore — or a pinned version could lose its log.
    delta_retention_days: int = Field(default=60, ge=1)
    restore_window_days: int = Field(default=30, ge=1)

    @model_validator(mode="after")
    def _refuse_unsafe_commits(self) -> "Settings":
        if self.aws_s3_allow_unsafe_rename:
            raise ValueError("AWS_S3_ALLOW_UNSAFE_RENAME is forbidden: concurrent commits would overwrite each other")
        return self

    @model_validator(mode="after")
    def _refuse_pins_longer_than_retention(self) -> "Settings":
        if self.delta_retention_days <= self.restore_window_days:
            raise ValueError(
                f"DELTA_RETENTION_DAYS ({self.delta_retention_days}) must exceed RESTORE_WINDOW_DAYS "
                f"({self.restore_window_days}): a pinned revision would lose its Delta log"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
