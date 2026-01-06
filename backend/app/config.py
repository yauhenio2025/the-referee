"""
Configuration settings for The Referee
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://localhost:5432/the_referee"

    # Redis for background jobs
    redis_url: str = "redis://localhost:6379/0"

    # API Keys (for external services)
    anthropic_api_key: str = ""
    oxylabs_username: str = ""
    oxylabs_password: str = ""

    # App settings
    app_name: str = "The Referee"
    debug: bool = False

    # CORS
    frontend_url: str = "http://localhost:5173"

    # External API Authentication
    # When enabled, external API endpoints require X-API-Key header
    api_auth_enabled: bool = False
    # Comma-separated list of valid API keys (e.g., "key1,key2,key3")
    api_keys: str = ""

    # Webhook settings
    webhook_timeout_seconds: int = 10
    webhook_max_retries: int = 3

    # Health Monitor settings
    health_monitor_enabled: bool = True
    health_monitor_interval_minutes: int = 5
    health_monitor_dry_run: bool = False  # If True, diagnose but don't execute actions

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    def get_api_keys_list(self) -> list[str]:
        """Parse comma-separated API keys into a list"""
        if not self.api_keys:
            return []
        return [k.strip() for k in self.api_keys.split(",") if k.strip()]


@lru_cache()
def get_settings() -> Settings:
    return Settings()
