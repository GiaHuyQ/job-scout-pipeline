import dagster as dg

# Pipeline to manage the ingestion from external sources into Bronze
bronze_jobs_pipeline = dg.define_asset_job(
    name="bronze_jobs_ingestion_pipeline_weekly",
    selection=dg.AssetSelection.groups("bronze"),
    description="Orchestrates the raw data ingestion (scraping) into the Bronze layer."
)

# Pipeline to manage the transformation from Bronze to Silver
silver_jobs_pipeline = dg.define_asset_job(
    name="silver_jobs_etl_pipeline_weekly",
    selection=dg.AssetSelection.groups("silver"),
    description="Orchestrates the ETL process: parsing raw HTML to structured JSON in the Silver layer."
)