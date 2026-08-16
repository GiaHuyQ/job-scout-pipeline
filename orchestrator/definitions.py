import os
from pathlib import Path

import dagster as dg
from dagster import (
    Definitions,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)

from orchestrator.assets import jobs_crawl, jobs_html_extract
from orchestrator.jobs import bronze_jobs_pipeline, silver_jobs_pipeline

# Import your custom modules
from orchestrator.resources.minio_resource import MinIOS3Resource
from orchestrator.schedulers import schedules

# ==========================================
# RESOURCE CONFIGURATION
# ==========================================
shared_minio_creds = {
    "endpoint_url": dg.EnvVar("MINIO_ENDPOINT_URL"),
    "aws_access_key_id": dg.EnvVar("MINIO_ROOT_USER"),
    "aws_secret_access_key": dg.EnvVar("MINIO_ROOT_PASSWORD"),
    "region_name": dg.EnvVar("MINIO_REGION")
}

# ==========================================
# DEFINITIONS
# ==========================================
defs = Definitions(
    # Automatically load all assets and asset_checks from these modules
    # This replaces the need to list 20+ variables manually
    assets=[
        *load_assets_from_modules([jobs_crawl, jobs_html_extract])
    ],
    
    asset_checks=[
        *load_asset_checks_from_modules([jobs_crawl, jobs_html_extract])
    ],

    # Register the jobs and schedules from your new files
    jobs=[
        bronze_jobs_pipeline, 
        silver_jobs_pipeline
    ],
    
    schedules=schedules,
    
    resources={
        "bronze_minio": MinIOS3Resource(s3_bucket="jobs-bronze", **shared_minio_creds),
        "silver_minio": MinIOS3Resource(s3_bucket="jobs-silver", **shared_minio_creds),
        "gold_minio": MinIOS3Resource(s3_bucket="jobs-gold", **shared_minio_creds)
    }
)

