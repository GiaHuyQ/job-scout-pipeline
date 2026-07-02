import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone

from config import settings
from workers.schemas import CrawlResult

logger = logging.getLogger(__name__)

def save_dual_bronze(result: CrawlResult, target_selector: str, user_agent: str) -> Path | None:
    """
    Writes a robust operational storage data triplet directly to filesystem layers.
    Accepts dynamic selectors and user agents to support multiple flexible workers.
    """
    if result.status != "ok" or result.screenshot_bytes is None or result.inner_html is None:
        return None

    settings.BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    url_hash = hashlib.sha256(result.url.encode()).hexdigest()[:16]
    
    metadata_manifest = {
        "data_ingestion_id": url_hash,
        "source_url": result.url,
        "search_context": {
            "query_keyword": result.query,
            "domain_scope": result.site
        },
        "extracted_page_title": result.title,
        "temporal_metrics": {
            "scraped_at_timestamp": result.fetched_at,
            "scraped_at_iso": datetime.now(timezone.utc).isoformat()
        },
        "payload_telemetry": {
            "data_format_type": "dual_visual_and_html_snippet",
            "html_character_length": len(result.inner_html),
            "screenshot_bytes_size": len(result.screenshot_bytes),
            "target_css_selector": target_selector,
            "image_encoding": "png"
        },
        "environment_provenance": {
            "user_agent_applied": user_agent,
            "pipeline_format_version": "1.2.0"
        }
    }

    image_path = settings.BRONZE_DIR / f"{url_hash}.png"
    html_path = settings.BRONZE_DIR / f"{url_hash}.html"
    meta_path = settings.BRONZE_DIR / f"{url_hash}.meta.json"

    image_path.write_bytes(result.screenshot_bytes)
    html_path.write_text(result.inner_html, encoding="utf-8")
    meta_path.write_text(json.dumps(metadata_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    
    logger.info("Real-time Ingestion Success: Triplets saved for hash id %s", url_hash)
    return html_path