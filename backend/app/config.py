"""Application configuration using Pydantic Settings."""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    reload: bool = False

    # CORS settings
    cors_origins: str = "*"

    # Default S3/MinIO settings (used when catalog doesn't specify)
    default_s3_endpoint: Optional[str] = None
    default_s3_access_key: Optional[str] = None
    default_s3_secret_key: Optional[str] = None
    default_s3_region: str = "us-east-1"

    # Default catalog auto-registration (for Docker Compose setup)
    default_catalog_name: Optional[str] = None
    default_catalog_type: str = "hive"
    default_catalog_uri: Optional[str] = None

    # Cache settings
    cache_enabled: bool = True
    cache_ttl_seconds: int = 300

    # Database settings
    fern_use_db: bool = False
    database_url: Optional[str] = None
    db_pool_size: int = 5
    db_max_overflow: int = 10
    replica_id: str = "local-dev"

    # Observability settings
    otel_exporter_otlp_endpoint: Optional[str] = None
    metrics_enabled: bool = True

    # MCP service/client settings
    fern_api_base_url: str = "http://localhost:8000"
    fern_mcp_token: Optional[str] = None
    mcp_request_timeout_seconds: float = 30.0
    mcp_max_response_bytes: int = 250_000

    @property
    def cors_origins_list(self) -> list[str]:
        """Parse CORS origins from comma-separated string."""
        if self.cors_origins == "*":
            return ["*"]
        return [origin.strip() for origin in self.cors_origins.split(",")]


# Global settings instance
settings = Settings()
