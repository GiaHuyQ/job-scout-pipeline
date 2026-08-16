import logging
import time
import pytz
from datetime import datetime
from typing import Callable, Awaitable

import aioboto3
import dagster as dg
from dagster import RetryPolicy, Backoff, AssetExecutionContext, AssetCheckExecutionContext, AssetCheckResult
from pydantic import Field

from orchestrator.resources.minio_resource import MinIOS3Resource
from workers.worker_topcv.scraper import run_crawl as topcv_crawl
from workers.worker_itviec.scraper import run_crawl as itviec_crawl
from workers.worker_careerviet.scraper import run_crawl as careerviet_crawl
from workers.worker_vietnamworks.scraper import run_crawl as vietnamworks_crawl

logger = logging.getLogger(__name__)

class JobCrawlConfig(dg.Config):
    keyword: str = "ai engineer"
    max_results: int = Field(default=10, ge=10, le=100)

anti_block_retry = RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL)

# --- Helpers ---

def get_fetched_at() -> str:
    return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime("%Y-%m-%d")

def count_s3_files(minio_resource: MinIOS3Resource, prefix: str) -> int:
    file_count = 0
    with minio_resource.get_sync_client() as s3_client:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=minio_resource.s3_bucket, Prefix=prefix):
            if "Contents" in page:
                file_count += len(page["Contents"])
    return file_count

# --- Base Logic (Reusable) ---

async def run_base_crawl(
    context: AssetExecutionContext, 
    bronze_minio: MinIOS3Resource, 
    config: JobCrawlConfig, 
    crawler_func: Callable[..., Awaitable[int]], 
    site_name: str
) -> dg.MaterializeResult:
    
    start_time = time.time()
    fetched_at = get_fetched_at()
    prefix = f"date={fetched_at}/query={config.keyword.replace(' ', '-')}/site={site_name}/"
    
    context.log.info(f"Starting {site_name} crawl. Keyword: '{config.keyword}'")
    
    session = aioboto3.Session()
    async with bronze_minio.get_client(session) as async_s3_client:
        result_count = await crawler_func(
            keyword=config.keyword, site=site_name, max_results=config.max_results,
            bucket=bronze_minio.s3_bucket, async_s3_client=async_s3_client
        )
    
    if result_count == 0:
        context.log.error(f"Crawler for {site_name} returned 0 results.")
        raise RuntimeError(f"No results for {site_name}. Triggering retry...")
        
    duration = round(time.time() - start_time, 2)
    context.log.info(f"Successfully ingested {result_count} items from {site_name} in {duration}s.")
    
    return dg.MaterializeResult(
        metadata={
            "query": dg.MetadataValue.text(config.keyword.replace(' ', '-')),
            "site": dg.MetadataValue.text(site_name),
            "jobs_count": dg.MetadataValue.int(result_count),
            "duration_sec": dg.MetadataValue.float(duration),
            "date": dg.MetadataValue.text(fetched_at),
            "prefix": dg.MetadataValue.text(prefix)
        }
    )

# --- Assets ---

@dg.asset(group_name="bronze", compute_kind="minio", retry_policy=anti_block_retry, code_version="20260812")
async def raw_topcv_jobs(context: AssetExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig):
    return await run_base_crawl(context, bronze_minio, config, topcv_crawl, "topcv.vn")

@dg.asset(group_name="bronze", compute_kind="minio", retry_policy=anti_block_retry, code_version="20260812")
async def raw_itviec_jobs(context: AssetExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig):
    return await run_base_crawl(context, bronze_minio, config, itviec_crawl, "itviec.com")

@dg.asset(group_name="bronze", compute_kind="minio", retry_policy=anti_block_retry, code_version="20260812")
async def raw_careerviet_jobs(context: AssetExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig):
    return await run_base_crawl(context, bronze_minio, config, careerviet_crawl, "careerviet.vn")

@dg.asset(group_name="bronze", compute_kind="minio", retry_policy=anti_block_retry, code_version="20260812")
async def raw_vietnamworks_jobs(context: AssetExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig):
    return await run_base_crawl(context, bronze_minio, config, vietnamworks_crawl, "vietnamworks.com")

# --- Asset Checks ---

def run_check(context: AssetCheckExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig, site_name: str) -> AssetCheckResult:
    fetched_at = get_fetched_at()
    prefix = f"date={fetched_at}/query={config.keyword.replace(' ', '-')}/site={site_name}/"
    
    context.log.info(f"Validating data for {site_name} at path: {prefix}")
    
    file_count = count_s3_files(bronze_minio, prefix)
    is_passed = file_count >= (config.max_results * 2)*0.75
    
    if not is_passed:
        context.log.error(f"Check failed! Found {file_count} files for {site_name}.")
        
    return AssetCheckResult(
        passed=is_passed,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "actual_files": dg.MetadataValue.int(file_count),
            "path": dg.MetadataValue.text(prefix)
        }
    )

@dg.asset_check(asset=raw_topcv_jobs)
def check_raw_topcv_jobs(context: AssetCheckExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig):
    return run_check(context, bronze_minio, config, "topcv.vn")

@dg.asset_check(asset=raw_itviec_jobs)
def check_raw_itviec_jobs(context: AssetCheckExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig):
    return run_check(context, bronze_minio, config, "itviec.com")

@dg.asset_check(asset=raw_careerviet_jobs)
def check_raw_careerviet_jobs(context: AssetCheckExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig):
    return run_check(context, bronze_minio, config, "careerviet.vn")

@dg.asset_check(asset=raw_vietnamworks_jobs)
def check_raw_vietnamworks_jobs(context: AssetCheckExecutionContext, bronze_minio: MinIOS3Resource, config: JobCrawlConfig):
    return run_check(context, bronze_minio, config, "vietnamworks.com")