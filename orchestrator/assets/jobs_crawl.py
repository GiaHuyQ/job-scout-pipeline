import logging
import aioboto3
import dagster as dg
from dagster import AssetExecutionContext
from orchestrator.resources.minio_resource import MinIOS3Resource

# Import your actual worker crawl functions
from workers.worker_topcv.scraper import run_crawl as topcv_crawl
from workers.worker_itviec.scraper import run_crawl as itviec_crawl
from workers.worker_careerviet.scraper import run_crawl as careerviet_crawl
from workers.worker_vietnamworks.scraper import run_crawl as vietnamworks_crawl

logger = logging.getLogger(__name__)

# Define global constants for the crawl jobs
SEARCH_KEYWORD = "ai engineer"
BRONZE_BUCKET = "jobs-bronze"


@dg.asset(
    group_name="bronze", 
    compute_kind=["minio"]
)
async def raw_topcv_jobs(context: AssetExecutionContext, minio_s3: MinIOS3Resource) -> str:
    """
    Crawl raw job data from TopCV and stream it to MinIO bronze bucket.
    """
    session = aioboto3.Session()
    
    context.log.info(f"Starting TopCV crawler for keyword: '{SEARCH_KEYWORD}'")
    
    # Open connection using the custom Dagster MinIO resource
    async with minio_s3.client(session) as async_s3_client:
        result_count = await topcv_crawl(
            keyword=SEARCH_KEYWORD,
            site="topcv.vn",
            max_results=50,
            bucket=BRONZE_BUCKET,
            async_s3_client=async_s3_client
        )
        
    context.log.info(f"TopCV crawling finished. Ingested {result_count} job targets.")
    return f"TopCV: Ingested {result_count} raw documents"


@dg.asset(
    group_name="bronze", 
    compute_kind=["minio"]
)
async def raw_itviec_jobs(context: AssetExecutionContext, minio_s3: MinIOS3Resource) -> str:
    """
    Crawl raw job data from ITViec and stream it to MinIO bronze bucket.
    """
    session = aioboto3.Session()
    
    context.log.info(f"Starting ITViec crawler for keyword: '{SEARCH_KEYWORD}'")
    
    # Reuse the same MinIO resource connection configuration
    async with minio_s3.lient(session) as async_s3_client:
        result_count = await itviec_crawl(
            keyword=SEARCH_KEYWORD,
            site="itviec.com",
            max_results=50,
            bucket=BRONZE_BUCKET,
            async_s3_client=async_s3_client
        )
        
    context.log.info(f"ITViec crawling finished. Ingested {result_count} job targets.")
    return f"ITViec: Ingested {result_count} raw documents"


@dg.asset(
    group_name="bronze", 
    compute_kind=["minio"],
)
async def raw_careerviet_jobs(context: AssetExecutionContext, minio_s3: MinIOS3Resource) -> str:
    """
    Crawl raw job data from CareerViet and stream it to MinIO bronze bucket.
    """
    session = aioboto3.Session()
    
    context.log.info(f"Starting CareerViet crawler for keyword: '{SEARCH_KEYWORD}'")
    
    # Reuse the same MinIO resource connection configuration
    async with minio_s3.lient(session) as async_s3_client:
        result_count = await careerviet_crawl(
            keyword=SEARCH_KEYWORD,
            site="careerviet.vn",
            max_results=50,
            bucket=BRONZE_BUCKET,
            async_s3_client=async_s3_client
        )
        
    context.log.info(f"CareerViet crawling finished. Ingested {result_count} job targets.")
    return f"CareerViet: Ingested {result_count} raw documents"


@dg.asset(
    group_name="bronze", 
    compute_kind=["minio"],
)
async def raw_vietnameworks_jobs(context: AssetExecutionContext, minio_s3: MinIOS3Resource) -> str:
    """
    Crawl raw job data from VietnamWorks and stream it to MinIO bronze bucket.
    """
    session = aioboto3.Session()
    
    context.log.info(f"Starting VietnamWorks crawler for keyword: '{SEARCH_KEYWORD}'")
    
    # Reuse the same MinIO resource connection configuration
    async with minio_s3.lient(session) as async_s3_client:
        result_count = await vietnamworks_crawl(
            keyword=SEARCH_KEYWORD,
            site="vietnamworks.com",
            max_results=50,
            bucket=BRONZE_BUCKET,
            async_s3_client=async_s3_client
        )
        
    context.log.info(f"ITViec crawling finished. Ingested {result_count} job targets.")
    return f"VietnamWorks: Ingested {result_count} raw documents"