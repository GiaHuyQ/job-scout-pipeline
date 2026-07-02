import random
import argparse
import asyncio
import logging
from urllib.parse import urljoin
from playwright.async_api import Browser, Page, async_playwright

from logging_config import setup_logging
from config import settings

from workers.schemas import CrawlTarget, CrawlResult
from workers.search import search_searxng
from workers.store import save_dual_bronze
from workers.interactions import USER_AGENT, smooth_scroll_targeted_element, purge_sticky_overlays, human_like_idle_scrolling

setup_logging(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)


SELECTOR_JOB_CONTAINER = "div.jd-main div.col-xl-8"
SELECTOR_LISTING_CARD = "div.card-jobs-list [data-url]"
SELECTOR_NEXT_PAGE = "nav.ipagination a:has-text('›')"

async def extract_inner_links(browser: Browser, main_target: CrawlTarget, limit: int) -> list[CrawlTarget]:
    """
    Scans ITViec reactive listing layouts with dynamic pagination loops 
    until the requested target budget limit is fully satisfied.
    """
    sub_targets = []
    context = None
    try:
        context = await browser.new_context()
        page: Page = await context.new_page()
        
        logger.info("Opening ITViec indexing layout viewport: %s", main_target.url)
        await page.goto(main_target.url, timeout=settings.PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
        
        extracted_urls = set()
        page_number = 1

        # Budget-driven Pagination Loop
        while len(extracted_urls) < limit:
            logger.info("Processing ITViec job listing page #%d (Current items: %d/%d)", page_number, len(extracted_urls), limit)
            
            try:
                # Ensure the job cards cluster is fully rendered on current page
                await page.wait_for_selector(SELECTOR_LISTING_CARD, timeout=8000)
            except Exception:
                logger.warning("No more listing cards found matching criteria on page #%d. Halting discovery loop.", page_number)
                break

            # Harvest all valid job targets on the current viewport
            cards = await page.locator(SELECTOR_LISTING_CARD).all()
            page_links_before = len(extracted_urls)
            
            for card in cards:
                if len(extracted_urls) >= limit:
                    break
                url_attr = await card.get_attribute("data-url")
                if url_attr:
                    clean_url = urljoin(main_target.url, url_attr).split("?")[0]
                    if "/viec-lam-it/" in clean_url or "/jobs/" in clean_url:
                        extracted_urls.add(clean_url)

            logger.info("Harvested %d new links from page #%d.", len(extracted_urls) - page_links_before, page_number)

            # Check if budget requirement is completely filled
            if len(extracted_urls) >= limit:
                logger.info("Target collection quota limit reached (%d items). Stopping pagination.", limit)
                break

            # Check for the existence of an active 'Next Page' interaction node
            next_button = page.locator(SELECTOR_NEXT_PAGE)
            if await next_button.count() > 0 and await next_button.is_visible() and await next_button.is_enabled():
                page_number += 1
                logger.info("Navigating to ITViec result index page #%d...", page_number)
                
                # Execute click and wait for network profile to settle (Crucial for SPA applications)
                await next_button.click()
                await page.wait_for_load_state("networkidle", timeout=5000)
                await page.wait_for_timeout(1000) # Safety breathing room
            else:
                logger.info("No further pagination navigators detected on DOM layout. Ending loop.")
                break

        for url in extracted_urls:
            sub_targets.append(CrawlTarget(url=url, query=main_target.query, site=main_target.site))
            
    except Exception as exc:
        logger.warning("ITViec paginated hyperlink extraction pipeline trace fault: %s", exc)
    finally:
        if context:
            await context.close()
            
    return sub_targets


async def fetch_and_save_worker(browser: Browser, target: CrawlTarget, semaphore: asyncio.Semaphore) -> None:
    """Worker lifecycle pipeline: Smooth scroll, clean-up, and snapshot persistence."""
    async with semaphore:
        context = None
        result = None
        try:
            context = await browser.new_context(viewport={"width": 1440, "height": 900})
            page: Page = await context.new_page()
            
            logger.info("Accessing target ITViec job detail module: %s", target.url)
      
            await page.goto(target.url, timeout=settings.PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            
            content_element = await page.wait_for_selector(SELECTOR_JOB_CONTAINER, timeout=10000)
            if not content_element:
                raise ValueError(f"Core ITViec target selector [{SELECTOR_JOB_CONTAINER}] missing.")

            # Execution of Bounded Smooth Scrolling & DOM Cleanups Macros
            await smooth_scroll_targeted_element(page, SELECTOR_JOB_CONTAINER)
            await purge_sticky_overlays(page)

            await content_element.scroll_into_view_if_needed()
            await page.wait_for_timeout(500)
            
            title = await page.title()
            
            result = CrawlResult(
                url=target.url, query=target.query, site=target.site, status="ok",
                screenshot_bytes=await content_element.screenshot(type="png"),
                inner_html=await content_element.inner_html(), title=title,
            )

            idle_duration = random.uniform(4.0, 10.0)
            await human_like_idle_scrolling(page, idle_duration)
            
        except Exception as exc:
            logger.warning("ITViec pipeline ingestion execution node fault traced on %s: %s", target.url, exc)
            result = CrawlResult(url=target.url, query=target.query, site=target.site, status="error", error=str(exc))
        finally:
            if context:
                await context.close()

        if result and result.status == "ok":
            save_dual_bronze(result, target_selector=SELECTOR_JOB_CONTAINER, user_agent=USER_AGENT)
            
        actual_delay = settings.REQUEST_DELAY_SEC * random.uniform(0.8, 1.2)
        logger.info("Enforcing anti-bot jitter delay: Sleeping for %.2f seconds...", actual_delay)
        await asyncio.sleep(actual_delay)


async def run_crawl(keyword: str, site: str | None, max_results: int) -> int:
    """Main sequence controller orchestrating ITViệc asynchronous worker clusters."""
    main_targets = search_searxng(keyword, site)
    if not main_targets:
        logger.warning("No candidate seed targets returned for ITViec. Halting pipelines.")
        return 0

    semaphore = asyncio.Semaphore(settings.CRAWL_CONCURRENCY)

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.connect_over_cdp(settings.CHROME_DEBUG_URL)
        except Exception as e:
            logger.error("ITViec worker unable to hook into port 9222: %s", e)
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
            logger.info("ITViec ingestion targets mapped. Queued: %d", len(unique_sub_targets))

            if not unique_sub_targets:
                return 0

            tasks = [fetch_and_save_worker(browser, t, semaphore) for t in unique_sub_targets]
            await asyncio.gather(*tasks)

            return len(unique_sub_targets)
        
        except Exception as exc:
            logger.error("ITViec global supervisor engine caught a logical fault: %s", exc, exc_info=True)
            return 0
        
        finally:
            await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming Real-Time Engine Custom Worker Module for ITViệc.")
    parser.add_argument("--keyword", required=True, help="Job lookup text query string")
    parser.add_argument("--site", default="itviec.com", help="Restrict mapping domain scope limit")
    parser.add_argument("--max-results", type=int, default=3, help="Result cap threshold metrics value")
    args = parser.parse_args()

    asyncio.run(run_crawl(args.keyword, args.site, args.max_results))


if __name__ == "__main__":
    main()