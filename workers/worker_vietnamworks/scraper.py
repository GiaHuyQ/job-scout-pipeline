import argparse
import asyncio
import logging
import random
from urllib.parse import urljoin
from playwright.async_api import Browser, Page, async_playwright

from logging_config import setup_logging
from config import settings

from workers.schemas import CrawlTarget, CrawlResult
from workers.search import search_searxng
from workers.store import save_dual_bronze
from workers.interactions import USER_AGENT, smooth_scroll_targeted_element, human_like_idle_scrolling

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


async def fetch_and_save_worker(browser: Browser, target: CrawlTarget, semaphore: asyncio.Semaphore) -> None:
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

        # Commit result to disk storage layer
        if result and result.status == "ok":
            save_dual_bronze(result, target_selector=SELECTOR_JOB_CONTAINER, user_agent=USER_AGENT)
            
        await asyncio.sleep(random.uniform(1.0, 2.5))


async def run_crawl(keyword: str, site: str | None, max_results: int) -> int:
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

            tasks = [fetch_and_save_worker(browser, t, semaphore) for t in unique_sub_targets]
            await asyncio.gather(*tasks)
  
            return len(unique_sub_targets)
        
        except Exception as exc:
            logger.error("VietnamWorks global supervisor engine caught a logical fault: %s", exc, exc_info=True)
            return 0
        
        finally:
            await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone Runner for VietnamWorks Worker.")
    parser.add_argument("--keyword", required=True)
    parser.add_argument("--site", default="vietnamworks.com")
    parser.add_argument("--max-results", type=int, default=3)
    args = parser.parse_args()

    asyncio.run(run_crawl(args.keyword, args.site, args.max_results))


if __name__ == "__main__":
    main()