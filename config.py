from pathlib import Path
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # --- SearXNG ---
    SEARXNG_URL: str = "http://localhost:8888/search"
 
    # --- Chrome Debug ---
    CHROME_DEBUG_URL: str = "http://127.0.0.1:9222"
 
    # --- Crawler behavior ---
    BRONZE_DIR: Path = Path("./data/bronze")
    CRAWL_CONCURRENCY: int = 2
    REQUEST_DELAY_SEC: float = 3.5
    PAGE_TIMEOUT_MS: int = 30_000

    # --- MinIO ---
    MINIO_ENDPOINT_URL: str = "http://localhost:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: SecretStr = SecretStr("")
    # --- Logging ---
    LOG_LEVEL: str = "INFO"

settings = Settings()