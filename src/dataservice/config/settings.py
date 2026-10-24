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

    @model_validator(mode="after")
    def _refuse_unsafe_commits(self) -> "Settings":
        if self.aws_s3_allow_unsafe_rename:
            raise ValueError("AWS_S3_ALLOW_UNSAFE_RENAME is forbidden: concurrent commits would overwrite each other")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
