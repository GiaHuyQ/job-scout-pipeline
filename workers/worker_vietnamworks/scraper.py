import argparse
import asyncio
import logging
import random
import hashlib
from urllib.parse import urljoin, quote

from logging_config import setup_logging
from config import settings
from workers.schemas import CrawlTarget, CrawlResult
from workers.search import search_searxng
from workers.interactions import smooth_scroll_targeted_element, human_like_idle_scrolling

from playwright.async_api import Browser, Page, async_playwright

setup_logging(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

# VietnamWorks Domain Specific Selectors Configurations
SELECTOR_LISTING_ANCHOR = "div.search-result-listJob a.img_job_card"
SELECTOR_NEXT_PAGE = "ul.pagination li.page-item button.clickable, ul.pagination li:last-child button"
SELECTOR_JOB_CONTAINER = "div.sc-d7132840-1.jZOjiN"

# Target both the 'Job Description' and 'Job Requirements' expand triggers
SELECTOR_EXPAND_BUTTONS = [
    "button.btn-info.clickable:has-text('Xem đầy đủ mô tả công việc')", 
    "button.btn-highlight.btn-md.clickable",
    "button.btn-highlight.btn-md.clickable"
]


async def extract_inner_links(browser: Browser, main_target: CrawlTarget, limit: int) -> list[CrawlTarget]:
    """Scans VietnamWorks layout with paginated loop logic until budget ceiling is satisfied."""
    sub_targets = []
    context = None
    try:
        context = await browser.new_context()
        page: Page = await context.new_page()
        
        logger.info("Opening VietnamWorks indexing viewport: %s", main_target.url)
        await page.goto(main_target.url, timeout=settings.PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
        
        extracted_urls = set()
        page_number = 1

        while len(extracted_urls) < limit:
            logger.info("Processing VietnamWorks listing page #%d (Items: %d/%d)", page_number, len(extracted_urls), limit)
            try:
                await page.wait_for_selector(SELECTOR_LISTING_ANCHOR, timeout=8000)
            except Exception:
                logger.warning("Listing anchors absent on page #%d. Halting exploration loop.", page_number)
                break

            anchors = await page.locator(SELECTOR_LISTING_ANCHOR).all()
            page_links_before = len(extracted_urls)
            
            for anchor in anchors:
                if len(extracted_urls) >= limit:
                    break
                href = await anchor.get_attribute("href")
                if href:
                    clean_url = urljoin("https://www.vietnamworks.com", href).split("?")[0]
                    extracted_urls.add(clean_url)

            logger.info("Harvested %d unique links from page #%d.", len(extracted_urls) - page_links_before, page_number)

            if len(extracted_urls) >= limit:
                break

            next_button = page.locator(SELECTOR_NEXT_PAGE).last
            if await next_button.count() > 0 and await next_button.is_visible():
                page_number += 1
                logger.info("Navigating to VietnamWorks listing index page #%d...", page_number)
                await next_button.click()
                await page.wait_for_load_state("domcontentloaded", timeout=5000)
                await page.wait_for_timeout(1500)
            else:
                logger.info("No further pagination controllers found on DOM tree. Ending loop.")
                break

        for url in extracted_urls:
            sub_targets.append(CrawlTarget(url=url, query=main_target.query, site=main_target.site))
            
    except Exception as exc:
        logger.warning("VietnamWorks paginated link discovery fault: %s", exc)
    finally:
        if context:
            await context.close()
            
    return sub_targets


async def fetch_and_stream_worker(browser: Browser, target: CrawlTarget, semaphore: asyncio.Semaphore, bucket: str, async_s3_client) -> None:
    """Worker node execution lifecycle with programmatic layout sanitization macros."""
    async with semaphore:
        context = None
        result = None
        try:
            context = await browser.new_context(viewport={"width": 1440, "height": 900})
            page: Page = await context.new_page()
            
            logger.info("Accessing target VietnamWorks job detail page: %s", target.url)
            await page.goto(target.url, timeout=settings.PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            
            content_element = await page.wait_for_selector(SELECTOR_JOB_CONTAINER, timeout=10000)
            if not content_element:
                raise ValueError(f"Core target layout element [{SELECTOR_JOB_CONTAINER}] vanished.")

            # Step 1: Pre-scroll to ensure content hydration and lazy-loaded items trigger
            await smooth_scroll_targeted_element(page, SELECTOR_JOB_CONTAINER)

            # Step 2: Programmatic DOM Expansion via JS-forced Evaluation (Immune to Interception)
            for selector in SELECTOR_EXPAND_BUTTONS:
                locators = page.locator(selector)
                count = await locators.count()
                
                for i in range(count):
                    btn = locators.nth(i)
                    if await btn.is_visible():
                        # Executes raw JavaScript click bypassing Playwright's collision/obscurity checks
                        await btn.evaluate("element => element.click()")
                        logger.info("Successfully forced click expansion on selector: [%s]", selector)
                        await page.wait_for_timeout(600)  # Sleep briefly to settle CSS animations

            # Step 3: Hard-purge all fixed/sticky floating banner overlays using computed styles
            await page.evaluate(f"""() => {{
                const allElements = document.querySelectorAll('body *');
                allElements.forEach(el => {{
                    const style = window.getComputedStyle(el);
                    if (style.position === 'fixed' || style.position === 'sticky') {{
                        // Guardrail: Avoid deleting our own core information viewport container
                        if (!el.contains(document.querySelector('{SELECTOR_JOB_CONTAINER}'))) {{
                            el.remove();
                        }}
                    }}
                }});
            }}""")
            logger.info("Aggressive cleaning of floating and fixed sticky layout banners completed.")

            # Final check to ensure viewport maps exactly to the sanitized target component bounds
            await content_element.scroll_into_view_if_needed()
            await page.wait_for_timeout(300)
            
            title = await page.title()
            screenshot_bytes = await content_element.screenshot(type="png")
            inner_html = await content_element.inner_html()
            
            result = CrawlResult(
                url=target.url, query=target.query, site=target.site, status="ok",
                screenshot_bytes=screenshot_bytes, inner_html=inner_html, title=title,
            )

            # Active human-like delay buffer tracking
            idle_duration = random.uniform(4.0, 10.0)
            await human_like_idle_scrolling(page, idle_duration)

        except Exception as exc:
            logger.warning("VietnamWorks ingestion node failed on URL %s: %s", target.url, exc)
            result = CrawlResult(url=target.url, query=target.query, site=target.site, status="error", error=str(exc))
        finally:
            if context:
                await context.close()

        # Commit result to streaming layers
        if result and result.status == "ok" and result.inner_html is not None and result.screenshot_bytes is not None:
            try:
                job_id  = hashlib.sha256(result.url.encode()).hexdigest()[:16]
                query = result.query.replace(" ", "-").lower()
                date_str = str(result.fetched_at.date())
                fetched_at_iso = result.fetched_at.isoformat()
                acsii_title = quote(str(result.title))

                html_key = f"date={date_str}/query={query}/site={result.site}/job_id={job_id}.html"
                png_key = f"date={date_str}/query={query}/site={result.site}/job_id={job_id}.png"

                html_metadata = {
                    "query": query,
                    "site": result.site,
                    "url": result.url,
                    "title": acsii_title,
                    "html_character_length": str(len(result.inner_html)),
                    "fetched_at": fetched_at_iso
                }

                png_metadata = {
                    "query": query,
                    "site": result.site,
                    "url": result.url,
                    "title": acsii_title,
                    "screenshot_bytes_size": str(len(result.screenshot_bytes)),
                    "fetched_at": fetched_at_iso
                }

                html_tagging = "Pipeline=JobScout&Stage=Bronze&DataType=HTML"
                png_tagging = "Pipeline=JobScout&Stage=Bronze&DataType=Image"

                logger.info("Streaming HTML directly from RAM to MinIO: %s", html_key)

                await async_s3_client.put_object(
                    Bucket=bucket,
                    Key=html_key,
                    Body=result.inner_html.encode("utf-8"),
                    ContentType="text/html",
                    Metadata=html_metadata,
                    Tagging=html_tagging

                )

                logger.info("Streaming PNG directly from RAM to MinIO: %s", png_key)

                await async_s3_client.put_object(
                    Bucket=bucket,
                    Key=png_key,
                    Body=result.screenshot_bytes,
                    ContentType="image/png",
                    Metadata=png_metadata,
                    Tagging=png_tagging
                )
            except Exception as s3_err:
                logger.error("Failed to stream artifacts to MinIO for %s: %s", target.url, s3_err)
            
        actual_delay = settings.REQUEST_DELAY_SEC * random.uniform(0.8, 1.2)
        logger.info("Enforcing anti-bot jitter delay: Sleeping for %.2f seconds...", actual_delay)
        await asyncio.sleep(actual_delay)


async def run_crawl(keyword: str, site: str | None, max_results: int, bucket: str, async_s3_client) -> int:
    """Main sequence controller orchestrating VietnamWorks asynchronous clusters."""
    main_targets = search_searxng(keyword, site)
    if not main_targets:
        logger.warning("No candidate seed targets returned for VietnamWorks. Halting.")
        return 0

    semaphore = asyncio.Semaphore(settings.CRAWL_CONCURRENCY)

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.connect_over_cdp(settings.CHROME_DEBUG_URL)
        except Exception as e:
            logger.error("Unable to connect to browser port 9222: %s", e)
            return 0

        try:
            all_sub_targets = []
            for main_target in main_targets:
                if len(all_sub_targets) >= max_results:
                    break
                remaining_budget = max_results - len(all_sub_targets)
                sub_links = await extract_inner_links(browser, main_target, remaining_budget)
                all_sub_targets.extend(sub_links)
                
            unique_sub_targets = {t.url: t for t in all_sub_targets}.values()
            logger.info("VietnamWorks targets mapped. Queued for execution: %d", len(unique_sub_targets))

            if not unique_sub_targets:
                return 0 

            tasks = [
                fetch_and_stream_worker(browser, t, semaphore, bucket, async_s3_client) 
                    for t in unique_sub_targets
            ]
            
            await asyncio.gather(*tasks)
  
            return len(unique_sub_targets)
        
        except Exception as exc:
            logger.error("VietnamWorks global supervisor engine caught a logical fault: %s", exc, exc_info=True)
            return 0
        
        finally:
            await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone Test Entrypoint Executable for CareerViet.")
    parser.add_argument("--keyword", required=True)
    parser.add_argument("--site", default="vietnamworks.com")
    parser.add_argument("--max-results", type=int, default=3)
    parser.add_argument("--bucket-name", type=str, default="jobs-bronze")
    args = parser.parse_args()
    
    import aioboto3
    from aiobotocore.config import AioConfig

    MINIO_CONFIG = {
    "endpoint_url": settings.MINIO_ENDPOINT_URL,
    "aws_access_key_id": settings.MINIO_ROOT_USER,
    "aws_secret_access_key": settings.MINIO_ROOT_PASSWORD.get_secret_value(),
    "config": AioConfig(signature_version="s3v4"),
    "region_name": "us-east-1"
}

    async def async_test_runner():
        session = aioboto3.Session()
        async with session.client("s3", **MINIO_CONFIG) as async_s3_client: # type: ignore
            await run_crawl(
                keyword=args.keyword, 
                site=args.site, 
                max_results=args.max_results, 
                bucket=args.bucket_name, 
                async_s3_client=async_s3_client
            )

    asyncio.run(async_test_runner())

if __name__ == "__main__":
    main()