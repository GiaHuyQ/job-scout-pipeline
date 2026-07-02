import argparse
import asyncio
import logging
import sys
import time
from typing import Any

from logging_config import setup_logging
from config import settings

from workers.worker_topcv import scraper as topcv_scraper
from workers.worker_itviec import scraper as itviec_scraper
from workers.worker_vietnamworks import scraper as vietnamworks_scraper
from workers.worker_careerviet import scraper as careerviet_scraper

# Initialize central logging orchestration framework
setup_logging(level=settings.LOG_LEVEL)
logger = logging.getLogger("pipeline_orchestrator")


async def execute_stage_with_retry(
    stage_id: str, 
    worker_module: Any, 
    keyword: str, 
    site_domain: str, 
    max_results: int, 
    max_retries: int = 3
) -> int:
    """
    Executes a specific worker scraping routine inside a resilient self-healing retry loop.
    Applies exponential backoff delays if zero items are recovered.
    """
    attempt = 1
    backoff_delay = 10.0  # Initial backoff floor in seconds

    while attempt <= max_retries:
        logger.info("[%s] Ingestion Attempt #%d/%d starting...", stage_id, attempt, max_retries)
        
        try:
            # Enforce a strict 10-minute timeout guardrail per attempt to prevent infinite hanging
            count = await asyncio.wait_for(
                worker_module.run_crawl(keyword=keyword, site=site_domain, max_results=max_results),
                timeout=600.0
            )
            
            # STRICT FIX: Explicitly evaluate count. If it's not a valid positive integer, force to 0.
            # This triggers the retry block immediately if the scraper returns None or 0.
            actual_count = count if (isinstance(count, int) and count > 0) else 0
            
            if actual_count > 0:
                logger.info("[%s] Successfully verified and saved %d records.", stage_id, actual_count)
                return actual_count
                
            logger.warning("[%s] Attempt #%d returned 0 records. Triggering retry sequence...", stage_id, attempt)
            
        except asyncio.TimeoutError:
            logger.critical("[%s] Attempt #%d aborted: Execution exceeded maximum 10-minute safety threshold.", stage_id, attempt)
        except Exception as exc:
            logger.error("[%s] Attempt #%d collapsed with exception: %s", stage_id, attempt, exc, exc_info=True)
            
        # If we reached here, the current attempt failed to yield records
        if attempt < max_retries:
            logger.info("[%s] Cooling down for %.1f seconds before triggering next retry node...", stage_id, backoff_delay)
            await asyncio.sleep(backoff_delay)
            backoff_delay *= 2.0  # Exponential increase (10s -> 20s -> 40s)
            
        attempt += 1

    logger.critical("[%s] All %d retrieval attempts exhausted. Marking node execution as failed.", stage_id, max_retries)
    return 0


async def run_pipeline(keyword: str, max_results: int) -> None:
    """Core controller orchestrating sequential stages with hardened metric layers."""
    global_start_time = time.time()
    cooldown_seconds = 45
    
    # Initialize real tracking storage maps
    metrics = {
        "TopCV": {"duration": 0.0, "count": 0, "status": "FAILED", "module": topcv_scraper, "domain": "topcv.vn/tim-viec-lam"},
        "ITViec": {"duration": 0.0, "count": 0, "status": "FAILED", "module": itviec_scraper, "domain": "itviec.com/viec-lam-it"},
        "VietnamWorks": {"duration": 0.0, "count": 0, "status": "FAILED", "module": vietnamworks_scraper, "domain": "vietnamworks.com"},
        "CareerViet": {"duration": 0.0, "count": 0, "status": "FAILED", "module": careerviet_scraper, "domain": "careerviet.vn/vi/tim-viec-lam/"}
    }

    logger.info("=" * 70)
    logger.info("INITIALIZING RESILIENT MULTI-SITE INGESTION PIPELINE")
    logger.info("Target Keyword Context: '%s' | Request Cap Per Site: %d", keyword, max_results)
    logger.info("=" * 70)

    stage_idx = 1
    total_stages = len(metrics)

    for site_name, config in metrics.items():
        logger.info("[STAGE %d/%d] Active Route Targeting Portal: %s", stage_idx, total_stages, site_name)
        stage_start = time.time()
        
        # Execute the stage inside our protective wrapper manager
        scraped_count = await execute_stage_with_retry(
            stage_id=f"STAGE {stage_idx}/{total_stages} - {site_name}",
            worker_module=config["module"],
            keyword=keyword,
            site_domain=config["domain"],
            max_results=max_results
        )
        
        # Commit honest records directly to the metrics map
        config["count"] = scraped_count
        config["duration"] = time.time() - stage_start
        
        # Evaluate clean status mappings
        if scraped_count >= max_results:
            config["status"] = "SUCCESS"
        elif scraped_count > 0:
            config["status"] = "PARTIAL"
        else:
            config["status"] = "FAILED"

        logger.info("[STAGE %d/%d] Segment Performance: %d items processed.", stage_idx, total_stages, scraped_count)
        
        # Enforce strategic de-escalation cooldown between distinct target domains
        if stage_idx < total_stages:
            logger.info("-" * 70)
            logger.info("Enforcing %d seconds stabilization cooldown buffer...", cooldown_seconds)
            logger.info("-" * 70)
            await asyncio.sleep(cooldown_seconds)
            
        stage_idx += 1

    # --------------------------------------------------------------------------
    # COMPREHENSIVE PIPELINE METRICS REPORT SUMMARY
    # --------------------------------------------------------------------------
    global_total_duration = time.time() - global_start_time
    total_scraped_jobs = sum(site["count"] for site in metrics.values())
    total_pure_worker_duration = sum(site["duration"] for site in metrics.values())

    logger.info("=" * 70)
    logger.info("                     HONEST INGESTION METRICS SUMMARY               ")
    logger.info("=" * 70)
    logger.info("  %-18s | %-10s | %-12s | %-10s", "Worker Site Name", "Status", "Duration", "Jobs Saved")
    logger.info("  " + "-" * 66)
    
    for site_name, data in metrics.items():
        # Guarantees perfect table grid alignment with matched header lengths
        logger.info(
            "  %-18s | %-10s | %-12.2fs | %-10d",
            site_name, data["status"], data["duration"], data["count"]
        )
        
    logger.info("  " + "-" * 66)
    logger.info("  Aggregated Pure Core Processing Time : %.2f seconds", total_pure_worker_duration)
    logger.info("  Aggregated Global Wall Clock Runtime : %.2f seconds (incl. cooldowns)", global_total_duration)
    logger.info("  Aggregated Successful Ingested Items : %d jobs", total_scraped_jobs)
    logger.info("=" * 70)


def main() -> None:
    """CLI routing arguments mapping configurations."""
    parser = argparse.ArgumentParser(
        description="Unified Multi-Worker Visual Scraping Engine Production Framework Orchestrator."
    )
    parser.add_argument("--keyword", required=True, help="Target job titles query lookup search parameters")
    parser.add_argument("--max-results", type=int, default=3, help="Result cap threshold per target portal")
    args = parser.parse_args()

    try:
        asyncio.run(run_pipeline(keyword=args.keyword, max_results=args.max_results))
    except KeyboardInterrupt:
        logger.warning("Pipeline manual termination triggered by standard user SIGINT. Exiting.")
        sys.exit(130)


if __name__ == "__main__":
    main()