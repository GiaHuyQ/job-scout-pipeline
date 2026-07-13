import dagster as dg
from orchestrator.jobs import bronze_jobs_pipeline, silver_jobs_pipeline

# ==========================================
# WEEKLY SCHEDULE DEFINITIONS
# ==========================================

# 1. Bronze Ingestion: Runs weekly on Saturday at 11:00 PM (23:00)
bronze_ingestion_schedule = dg.ScheduleDefinition(
    job=bronze_jobs_pipeline,
    cron_schedule="0 23 * * 6",  # Saturday at 23:00
    execution_timezone="Asia/Ho_Chi_Minh",
    description="Weekly ingestion of raw job data into Bronze bucket every Saturday at 23:00."
)

# 2. Silver ETL: Runs weekly on Sunday at 3:00 AM
# This allows the ingestion job to finish safely before transformation starts
silver_etl_schedule = dg.ScheduleDefinition(
    job=silver_jobs_pipeline,
    cron_schedule="0 3 * * 7",  # Sunday at 03:00
    execution_timezone="Asia/Ho_Chi_Minh",
    description="Weekly transformation of Bronze data to Silver JSON every Sunday at 03:00."
)

schedules = [
    bronze_ingestion_schedule,
    silver_etl_schedule
]