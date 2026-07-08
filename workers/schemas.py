from datetime import datetime, timezone
from dataclasses import dataclass, field

@dataclass
class CrawlTarget:
    """Represents a validated ingestion candidate endpoint."""
    url: str
    query: str
    site: str | None = None

@dataclass
class CrawlResult:
    """Encapsulates raw payload artifacts harvested during execution pipelines."""
    url: str
    query: str
    site: str | None
    status: str
    screenshot_bytes: bytes | None = None
    inner_html: str | None = None
    title: str | None = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    error: str | None = None

