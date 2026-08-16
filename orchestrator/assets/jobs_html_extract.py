import re
import json
from datetime import datetime, timedelta
from typing import Callable, Optional
from urllib.parse import quote, unquote

import dagster as dg
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

from orchestrator.assets.jobs_crawl import raw_careerviet_jobs, raw_itviec_jobs, raw_topcv_jobs, raw_vietnamworks_jobs
from orchestrator.resources.minio_resource import MinIOS3Resource


class JobModel(BaseModel):
    """Pydantic model representing the structure of a processed job posting."""
    title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None
    job_type: Optional[str] = None
    salary: Optional[str] = None
    job_level: Optional[str] = None
    deadline: Optional[str] = None
    description: str = ""
    description_text: str = ""  
    requirements: str = ""
    requirements_text: str = ""  
    link: Optional[str] = None
    ingested_at: str = Field(default_factory=lambda: datetime.now().isoformat())


def clean_html_to_text(html_content: str) -> str:
    """Converts HTML content to clean text, preserving list structure."""
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Convert list items to bullet points for better LLM context
    for li in soup.find_all('li'):
        li.insert_before("- ")
        
    return soup.get_text(separator='\n', strip=True)


# ==========================================
# EXTRACTION FUNCTIONS
# ==========================================

def extract_careerviet_job_data(html_content: str, metadata: dict) -> JobModel:
    """Parses HTML content from CareerViet to build a JobModel instance."""
    html_content = unquote(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')

    # 1. Extract title

    title_tag = soup.find("h1", class_="title") or soup.find("h1")

    if title_tag:
        title = title_tag.get_text(strip=True)
    else:
        first_tag = soup.select_one(".job-tags ul li a")
        title = first_tag.get("title") if first_tag else None

    # 2. Extract Salary, Level, Type, Deadline
    raw_data = {}

    for li in soup.select(".detail-box li"):
        strong = li.find("strong")

        if not strong:
            continue

        label = strong.get_text(strip=True).replace(":", "").lower()
        p_tag = li.find("p")
        val = p_tag.get_text(strip=True) if p_tag else ""

        if "salary" in label or "lương" in label:
            raw_data["salary"] = val

        elif "job level" in label or "cấp bậc" in label:
            raw_data["job_level"] = val

        elif "job type" in label or "hình thức" in label:
            raw_data["job_type"] = val

        elif "deadline" in label or "hết hạn nộp" in label:
            raw_data["deadline"] = val

    # 3. Extract description & requirements
    desc_html, reqs_html = "", ""

    for row in soup.select(".detail-row"):
        title_tag_row = row.find("h2", class_="detail-title")

        if not title_tag_row:
            continue

        content = row.find("div")
        content_html = content.decode_contents() if content else ""

        section_title = title_tag_row.get_text(strip=True)

        if "Job Description" in section_title or "Mô tả Công việc" in section_title:
            desc_html = content_html

        elif "Job Requirement" in section_title or "Yêu Cầu Công Việc" in section_title:
            reqs_html = content_html

    # 4. Extract location
    map_link = soup.select_one(".map p a")
    location = map_link.text.strip() if map_link else None

    # 5. Handle metadata
    raw_company = metadata.get("title") or metadata.get("x-amz-meta-title") or ""
    link = metadata.get("url") or metadata.get("x-amz-meta-url")

    return JobModel(
        title=title,
        company=unquote(raw_company),
        link=link,
        location=location,
        salary=raw_data.get("salary"),
        job_type=raw_data.get("job_type"),
        job_level=raw_data.get("job_level"),
        deadline=raw_data.get("deadline"),
        description=desc_html,
        description_text=clean_html_to_text(desc_html),
        requirements=reqs_html,
        requirements_text=clean_html_to_text(reqs_html),
    )


def extract_topcv_job_data(html_content: str, metadata: dict) -> JobModel:
    """Parses HTML content from TopCV to build a JobModel instance."""
    html_content = unquote(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')

    # 1. Extract title
    title_tag = soup.select_one("h1.box-header-job__title")

    title = title_tag.get_text(" ", strip=True) if title_tag else None

    # 2. Extract company
    company_tag = soup.select_one(".box-company-info__detail .name")
    company = company_tag.get_text(strip=True) if company_tag else None

    # 3. Extract salary
    salary_tag = soup.select_one(".box-header-job__salary--title")
    salary = salary_tag.get_text(strip=True) if salary_tag else None

    # 4. Extract location
    location = None

    for item in soup.select(".box-header-job-list-info__item"):
        title_node = item.select_one(".list-info__content__title")
        value_node = item.select_one(".list-info__content__desc")

        if not title_node or not value_node:
            continue

        key = title_node.get_text(strip=True)
        val = value_node.get_text(" ", strip=True)

        if key == "Địa điểm":
            location = val
            break

    # 5. Extract deadline
    deadline_node = soup.select_one(".box-applied-cv .date")
    deadline = deadline_node.get_text(strip=True) if deadline_node else None

    # 6. Extract description and requirements
    desc_html, reqs_html = "", ""

    for item in soup.select(".box-job-information-detail-item"):
        title_node = item.select_one(
            ".box-job-information-detail-item__title--title"
        )

        if not title_node:
            continue

        section_title = title_node.get_text(strip=True)

        content = item.select_one(
            ".box-job-information-detail-item__text"
        )

        content_html = content.decode_contents() if content else ""

        if "Mô tả công việc" in section_title:
            desc_html = content_html

        elif "Yêu cầu ứng viên" in section_title:
            reqs_html = content_html

    # 7. Handle metadata
    raw_company = metadata.get("title") or metadata.get("x-amz-meta-title") or ""
    link = metadata.get("url") or metadata.get("x-amz-meta-url")

    return JobModel(
        title=title,
        company=company or unquote(raw_company),
        link=link,
        location=location,
        salary=salary,
        job_type=None,
        job_level=None,
        deadline=deadline,
        description=desc_html,
        description_text=clean_html_to_text(desc_html),
        requirements=reqs_html,
        requirements_text=clean_html_to_text(reqs_html),
    )


def extract_itviec_job_data(html_content: str, metadata: dict) -> JobModel:
    """Parses HTML content from ITViec to build a JobModel instance."""
    html_content = unquote(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')

    # 1. Extract basic info
    title_node = soup.select_one("h1.text-it-black")
    title = title_node.get_text(strip=True) if title_node else None
    
    company_node = soup.select_one("div.employer-name")
    raw_company = company_node.get_text(strip=True) if company_node else ""
    
    link = metadata.get("url") or metadata.get("x-amz-meta-url")

    # 2. Build info map (Location & Salary)
    info_map = {}
    
    # Extract salary
    salary_tag = soup.select_one("a.sign-in-view-salary")
    info_map["Mức lương"] = salary_tag.get_text(strip=True) if salary_tag else "Thỏa thuận"
    
    # Extract location (finding the div containing the map-pin icon)
    location = None
    for div in soup.select("div.d-inline-block.text-dark-grey"):
        svg_use = div.find("use")
        if svg_use and "map-pin" in svg_use.get("href", ""):
            span = div.select_one("span.normal-text")
            if span:
                location = span.get_text(strip=True)
                break
    info_map["Địa điểm"] = location

    # Deadline (ITViec rarely displays explicit deadlines in this HTML structure)
    deadline = None 

    # 3. Extract description and requirements
    desc_html, reqs_html = "", ""
    for section in soup.select(".paragraph"):
        h2 = section.find("h2")
        if not h2: 
            continue
        
        h2_text = h2.get_text(strip=True)
        content = "".join([str(child) for child in section.find_all(recursive=False) if child.name != 'h2'])
        
        if "Mô tả công việc" in h2_text:
            desc_html = content.strip()
        elif "Yêu cầu công việc" in h2_text:
            reqs_html = content.strip()

    return JobModel(
        title=title,
        company=unquote(raw_company),
        link=link,
        location=info_map.get("Địa điểm"),
        salary=info_map.get("Mức lương"),
        job_type=None, 
        job_level=None,
        deadline=deadline,
        description=desc_html,
        description_text=clean_html_to_text(desc_html),
        requirements=reqs_html,
        requirements_text=clean_html_to_text(reqs_html),
    )


def extract_vietnamworks_job_data(html_content: str, metadata: dict) -> JobModel:
    """Parses HTML content from VietnamWorks to build a JobModel instance."""
    html_content = unquote(html_content)
    soup = BeautifulSoup(html_content, 'html.parser')

    # 1. Title: Extract using the specific CSS selector provided
    title_tag = soup.find("h1", attrs={"name": "title"}) 
    if not title_tag:
        title_tag = soup.find("h1", class_=re.compile(r"hAejeW|sc-ab270149-0"))
        
    title = title_tag.get_text(strip=True) if title_tag else metadata.get("title")

    # 2. Company: Extracted directly from reliable metadata
    raw_company = metadata.get("title", "N/A")
    company = unquote(raw_company)
    
    # 3. Location: Attempt exact structural selector, then fallback to label search
    location = None
    loc_element = soup.select_one("div.jRMnbF:nth-of-type(3) > div:nth-of-type(1) > span:nth-of-type(2)")
    
    if loc_element:
        location = loc_element.get_text(strip=True)
    else:
        # Fallback: Find "Địa điểm" label and extract adjacent text
        loc_label = soup.find(string=re.compile(r"Địa điểm", re.IGNORECASE))
        if loc_label:
            parent = loc_label.find_parent("div")
            location = parent.get_text(strip=True).replace("Địa điểm", "").strip()

    # 4. Salary: Based on specific #FF7D55 color code used by VietnamWorks UI
    salary_tag = soup.find("span", attrs={"color": "#FF7D55"})
    salary = salary_tag.get_text(strip=True) if salary_tag else "Thỏa thuận"

    # 5. Deadline: Usually calculated as 14 days post publication date
    posted_date_text = soup.find(string=re.compile(r"NGÀY ĐĂNG", re.IGNORECASE))
    deadline = None
    if posted_date_text:
        parent = posted_date_text.find_parent()
        date_val = parent.find_next_sibling() or parent.parent.find_next_sibling()
        if date_val:
            try:
                post_date = datetime.strptime(date_val.get_text(strip=True), "%d/%m/%Y")
                deadline = (post_date + timedelta(days=14)).strftime("%d/%m/%Y")
            except ValueError:
                pass

    # 6. Extract requirements block
    reqs_html = ""
    req_header = soup.find(lambda tag: tag.name == 'h2' and "yêu cầu" in tag.get_text().lower())
    if req_header:
        container = req_header.find_parent()
        content_div = container.find("div", class_=re.compile(r"sc-1671001a-6"))
        if content_div:
            reqs_html = str(content_div)

    # 7. Extract description block using the same structural logic
    desc_html = ""
    desc_header = soup.find(lambda tag: tag.name == 'h2' and "mô tả" in tag.get_text().lower())
    if desc_header:
        container = desc_header.find_parent()
        content_div = container.find("div", class_=re.compile(r"sc-1671001a-6"))
        if content_div:
            desc_html = str(content_div)

    return JobModel(
        title=title,
        company=company,
        location=location,
        salary=salary,
        deadline=deadline,
        description=desc_html,
        description_text=clean_html_to_text(desc_html),
        requirements=reqs_html,
        requirements_text=clean_html_to_text(reqs_html),
        link=metadata.get("url"),
        ingested_at=datetime.now().isoformat()
    )


# ==========================================
# REUSABLE DAGSTER S3 PROCESSOR
# ==========================================

def process_job_site_assets(
    context: dg.AssetExecutionContext,
    bronze_minio: MinIOS3Resource,
    silver_minio: MinIOS3Resource,
    site_domain: str,
    extractor_func: Callable[[str, dict], JobModel]
) -> dg.MaterializeResult:
    """
    A generic helper function to read raw HTML files from Bronze S3, 
    parse them using the provided extractor function, and save the 
    resulting JSON models into Silver S3 with metadata and tags.
    """
    fetched_at = datetime.now().strftime("%Y-%m-%d")
    query = "ai-engineer"  # Note: Can be sourced from Dagster config in the future
    prefix = f"date={fetched_at}/query={query}/site={site_domain}/"

    with bronze_minio.get_sync_client() as s3_client:
        objects = s3_client.list_objects_v2(Bucket=bronze_minio.s3_bucket, Prefix=prefix).get('Contents', [])
        count = 0

        for obj in objects:
            key = obj['Key']
            if not key.endswith(".html"):
                continue

            # 1. Fetch raw data and metadata
            head = s3_client.head_object(Bucket=bronze_minio.s3_bucket, Key=key)
            metadata = head.get('Metadata', {})
            resp = s3_client.get_object(Bucket=bronze_minio.s3_bucket, Key=key)
            html_raw = resp['Body'].read().decode('utf-8')

            # 2. Extract structured data
            job_instance = extractor_func(html_raw, metadata)

            # 3. Define Silver Metadata (S3 headers are inherently lowercase)
            silver_metadata = {
                "query": query,
                "site": site_domain,
                "url": job_instance.link or "",
                "title": quote(job_instance.company or "unknown")
            }

            # 4. Define specific tags requested for pipeline tracking
            tagging_str = "Pipeline=JobScout&Stage=Silver&DataType=Json"

            # 5. Store parsed JSON in Silver bucket
            silver_key = "html/" + key.replace(".html", ".json")

            with silver_minio.get_sync_client() as silver_client:
                silver_client.put_object(
                    Bucket=silver_minio.s3_bucket,
                    Key=silver_key,
                    Body=job_instance.model_dump_json(indent=2),
                    ContentType='application/json',
                    Metadata=silver_metadata,
                    Tagging=tagging_str
                )
            
            count += 1
            context.log.info(f"Cleaned and saved [{site_domain}]: {silver_key}")

    return dg.MaterializeResult(metadata={"processed_files": dg.MetadataValue.int(count)})


# ==========================================
# DAGSTER ASSETS
# ==========================================

@dg.asset(group_name="silver", compute_kind="minio", deps=[raw_careerviet_jobs], code_version="20260812")
def cleanned_html_careerviet_jobs(context: dg.AssetExecutionContext, bronze_minio: MinIOS3Resource, silver_minio: MinIOS3Resource):
    """Processes CareerViet raw HTML, extracts data, and saves to Silver."""
    return process_job_site_assets(context, bronze_minio, silver_minio, "careerviet.vn", extract_careerviet_job_data)


@dg.asset(group_name="silver", compute_kind="minio", deps=[raw_topcv_jobs], code_version="20260812")
def cleanned_html_topcv_jobs(context: dg.AssetExecutionContext, bronze_minio: MinIOS3Resource, silver_minio: MinIOS3Resource):
    """Processes TopCV raw HTML, extracts data, and saves to Silver."""
    return process_job_site_assets(context, bronze_minio, silver_minio, "topcv.vn", extract_topcv_job_data)


@dg.asset(group_name="silver", compute_kind="minio", deps=[raw_itviec_jobs], code_version="20260710")
def cleanned_html_itviec_jobs(context: dg.AssetExecutionContext, bronze_minio: MinIOS3Resource, silver_minio: MinIOS3Resource):
    """Processes ITViec raw HTML, extracts data, and saves to Silver."""
    return process_job_site_assets(context, bronze_minio, silver_minio, "itviec.com", extract_itviec_job_data)


@dg.asset(group_name="silver", compute_kind="minio", deps=[raw_vietnamworks_jobs], code_version="20260710")
def cleanned_html_vietnamworks_jobs(context: dg.AssetExecutionContext, bronze_minio: MinIOS3Resource, silver_minio: MinIOS3Resource):
    """Processes VietnamWorks raw HTML, extracts data, and saves to Silver."""
    return process_job_site_assets(context, bronze_minio, silver_minio, "vietnamworks.com", extract_vietnamworks_job_data)


# Define the fields that must not be null or empty
REQUIRED_FIELDS = [
    "title", "company", "location", "salary", 
    "description", "requirements", "link", "ingested_at"
]

def validate_silver_jobs_data(
    context: dg.AssetCheckExecutionContext,
    silver_minio: MinIOS3Resource,
    site_domain: str
) -> dg.AssetCheckResult:
    """
    Generic helper to validate Silver JSON files in S3.
    Checks if at least one file exists and ensures required fields are not null/empty.
    """
    fetched_at = datetime.now().strftime("%Y-%m-%d")
    query = "ai-engineer"
    prefix = f"html/date={fetched_at}/query={query}/site={site_domain}/"

    with silver_minio.get_sync_client() as s3_client:
        objects = s3_client.list_objects_v2(Bucket=silver_minio.s3_bucket, Prefix=prefix).get('Contents', [])
        
        # 1. Check if sufficient tasks/files exist
        if not objects:
            return dg.AssetCheckResult(
                passed=False,
                metadata={"error": "No files found in Silver layer for this date/query."}
            )

        total_files = 0
        invalid_files = []

        for obj in objects:
            key = obj['Key']
            if not key.endswith(".json"):
                continue
            
            total_files += 1
            
            # Fetch and parse JSON
            resp = s3_client.get_object(Bucket=silver_minio.s3_bucket, Key=key)
            file_content = resp['Body'].read().decode('utf-8')
            
            try:
                data = json.loads(file_content)
            except json.JSONDecodeError:
                invalid_files.append({"key": key, "reason": "Invalid JSON format"})
                continue

            # 2. Check for null or empty values in required fields
            missing_or_null = []
            for field in REQUIRED_FIELDS:
                val = data.get(field)
                # Check if value is None or an empty string
                if val is None or str(val).strip() == "": 
                    missing_or_null.append(field)
            
            if missing_or_null:
                invalid_files.append({
                    "key": key, 
                    "reason": f"Null/Empty fields: {', '.join(missing_or_null)}"
                })

        # The check passes only if there are files processed and ZERO invalid files
        is_passed = (total_files > 0) and (len(invalid_files) == 0)

        # Truncate invalid files list for Dagster UI if it's too large
        sample_invalid_files = invalid_files[:10] if invalid_files else []

        return dg.AssetCheckResult(
            passed=is_passed,
            metadata={
                "total_files_checked": dg.MetadataValue.int(total_files),
                "invalid_files_count": dg.MetadataValue.int(len(invalid_files)),
                "invalid_files_sample": dg.MetadataValue.json(sample_invalid_files)
            }
        )

# ==========================================
# DAGSTER ASSET CHECKS
# ==========================================

@dg.asset_check(asset="cleanned_html_careerviet_jobs", description="Ensure no null fields and sufficient data for CareerViet")
def check_careerviet_jobs_quality(context: dg.AssetCheckExecutionContext, silver_minio: MinIOS3Resource):
    return validate_silver_jobs_data(context, silver_minio, "careerviet.vn")

@dg.asset_check(asset="cleanned_html_topcv_jobs", description="Ensure no null fields and sufficient data for TopCV")
def check_topcv_jobs_quality(context: dg.AssetCheckExecutionContext, silver_minio: MinIOS3Resource):
    return validate_silver_jobs_data(context, silver_minio, "topcv.vn")

@dg.asset_check(asset="cleanned_html_itviec_jobs", description="Ensure no null fields and sufficient data for ITViec")
def check_itviec_jobs_quality(context: dg.AssetCheckExecutionContext, silver_minio: MinIOS3Resource):
    return validate_silver_jobs_data(context, silver_minio, "itviec.com")

@dg.asset_check(asset="cleanned_html_vietnamworks_jobs", description="Ensure no null fields and sufficient data for VietnamWorks")
def check_vietnamworks_jobs_quality(context: dg.AssetCheckExecutionContext, silver_minio: MinIOS3Resource):
    return validate_silver_jobs_data(context, silver_minio, "vietnamworks.com")