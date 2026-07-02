import argparse
import asyncio
import logging
import random
from urllib.parse import urljoin
from playwright.async_api import Browser, Page, async_playwright

from config import settings

from workers.schemas import CrawlTarget, CrawlResult
from workers.search import search_searxng
from workers.store import save_dual_bronze
from workers.interactions import USER_AGENT, smooth_scroll_targeted_element, human_like_idle_scrolling

logger = logging.getLogger(__name__)

# CareerViet Domain Specific Semantic Selectors
SELECTOR_LISTING_ANCHOR = "div.main-slide a.job_link"
SELECTOR_NEXT_PAGE = "div.pagination li.next-page a"

# FIX: Target the exact semantic core section container as requested
SELECTOR_JOB_CONTAINER = "section.job-detail-content"


async def extract_inner_links(browser: Browser, main_target: CrawlTarget, limit: int) -> list[CrawlTarget]:
    """Scans CareerViet listing pagination tree to extract canonical target URLs."""
    sub_targets = []
    context = None
    try:
        context = await browser.new_context()
        page: Page = await context.new_page()
        
        logger.info("Opening CareerViet indexing layout viewport: %s", main_target.url)
        await page.goto(main_target.url, timeout=settings.PAGE_TIMEOUT_MS, wait_until="load")
        
        extracted_urls = set()
        page_number = 1

        while len(extracted_urls) < limit:
            logger.info("Processing CareerViet listing page #%d (Current items: %d/%d)", page_number, len(extracted_urls), limit)
            try:
                await page.wait_for_selector(SELECTOR_LISTING_ANCHOR, timeout=8000)
            except Exception:
                logger.warning("No more listing anchors discovered on DOM page #%d. Terminating loop.", page_number)
                break

            anchors = await page.locator(SELECTOR_LISTING_ANCHOR).all()
            page_links_before = len(extracted_urls)
            
            for anchor in anchors:
                if len(extracted_urls) >= limit:
                    break
                href = await anchor.get_attribute("href")
                if href:
                    clean_url = urljoin("https://careerviet.vn", href).split("?")[0]
                    extracted_urls.add(clean_url)

            logger.info("Extracted %d fresh unique links from page #%d.", len(extracted_urls) - page_links_before, page_number)

            if len(extracted_urls) >= limit:
                break

            next_button = page.locator(SELECTOR_NEXT_PAGE)
            if await next_button.count() > 0 and await next_button.is_visible():
                page_number += 1
                logger.info("Transitioning to CareerViet index list page #%d...", page_number)
                await next_button.click()
                await page.wait_for_load_state("networkidle", timeout=5000)
                await page.wait_for_timeout(1000)
            else:
                logger.info("Pagination boundary limits exhausted on CareerViet tree node. Ending loop.")
                break

        for url in extracted_urls:
            sub_targets.append(CrawlTarget(url=url, query=main_target.query, site=main_target.site))
            
    except Exception as exc:
        logger.warning("CareerViet links discovery routing trace fault occurred: %s", exc)
    finally:
        if context:
            await context.close()
            
    return sub_targets

async def fetch_and_save_worker(browser: Browser, target: CrawlTarget, semaphore: asyncio.Semaphore) -> None:
    """Worker node lifecycle: Isolates target section, prunes noise nodes, and captures snapshots."""
    async with semaphore:
        context = None
        result = None
        try:
            context = await browser.new_context(viewport={"width": 1440, "height": 900})
            page: Page = await context.new_page()
            
            logger.info("Accessing target CareerViet job details section: %s", target.url)
            
            # HARD FIX: Change wait_until from "networkidle" to "domcontentloaded" 
            # This completely bypasses un-ending ad scripts and long-polling tracker sockets
            await page.goto(target.url, timeout=settings.PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            
            # Enforce strict element locator initialization matching the section node
            container_locator = page.locator(SELECTOR_JOB_CONTAINER).first
            
            # Wait for the actual job description content box to become visible on screen
            await container_locator.wait_for(state="visible", timeout=15000)
            
            # Give it a small 1.5-second breathing room for dynamic fonts and images to render completely
            await page.wait_for_timeout(1500)

            # Trigger smooth layout tracking to hydrate lazy elements natively
            await smooth_scroll_targeted_element(page, SELECTOR_JOB_CONTAINER)

            # ADVANCED PRUNING MACRO: Eradicate fixed components AND inner recommended job noise nodes
            await page.evaluate(f"""() => {{
                // 1. Purge floating sticky headers/footers
                const components = document.querySelectorAll('body *');
                components.forEach(el => {{
                    const css = window.getComputedStyle(el);
                    if (css.position === 'fixed' || css.position === 'sticky') {{
                        if (!el.contains(document.querySelector('{SELECTOR_JOB_CONTAINER}'))) {{
                            el.remove();
                        }}
                    }}
                }});

                // 2. HARD CORRECTION: Excise the internal similar jobs pollution block entirely
                const relatedJobsBlock = document.getElementById('related-jobs-new');
                if (relatedJobsBlock) {{
                    relatedJobsBlock.remove();
                }}

                // 3. Remove social widget sharing rows to maximize text extraction purity
                const shareWidget = document.querySelector('.share-this-job');
                if (shareWidget) {{
                    shareWidget.remove();
                }}
            }}""")
            logger.info("[CareerViet] Internal noise nodes and floating banners aggressively pruned from DOM.")

            # Frame the virtual camera precisely onto the cleansed element box bounds
            await container_locator.scroll_into_view_if_needed()
            await page.wait_for_timeout(500)  # Short pause to let rendering frame buffer finalize
            
            title = await page.title()
            screenshot_bytes = await container_locator.screenshot(type="png")
            inner_html = await container_locator.inner_html()
            
            result = CrawlResult(
                url=target.url, query=target.query, site=target.site, status="ok",
                screenshot_bytes=screenshot_bytes, inner_html=inner_html, title=title,
            )

            # Simulation delay buffer
            idle_duration = random.uniform(4.0, 10.0)
            await human_like_idle_scrolling(page, idle_duration)

        except Exception as exc:
            logger.warning("CareerViet extraction pipeline collapsed on node URL %s: %s", target.url, exc)
            result = CrawlResult(url=target.url, query=target.query, site=target.site, status="error", error=str(exc))
        finally:
            if context:
                await context.close()

        # Commit pristine payload to local storage and streaming layers
        if result and result.status == "ok":
            save_dual_bronze(result, target_selector=SELECTOR_JOB_CONTAINER, user_agent=USER_AGENT)
            
        await asyncio.sleep(random.uniform(1.5, 3.5))


async def run_crawl(keyword: str, site: str | None, max_results: int) -> int:
    """Main sequence controller governing CareerViet asynchronous worker execution tasks."""
    raw_targets = search_searxng(keyword, site, max_results=5)
    main_targets = [t for t in raw_targets if "/wiki-career/" not in t.url and "/talentcommunity/" not in t.url]
    
    if not main_targets:
        logger.warning("No valid candidate listing seed targets returned for CareerViet. Halting.")
        return 0

    semaphore = asyncio.Semaphore(settings.CRAWL_CONCURRENCY)

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.connect_over_cdp(settings.CHROME_DEBUG_URL)
        except Exception as e:
            logger.error("Unable to hook into chromium debug cluster channel via port 9222: %s", e)
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
            logger.info("CareerViet task distribution graph compiled. Queued nodes: %d", len(unique_sub_targets))

            if not unique_sub_targets:
                return 0

            tasks = [fetch_and_save_worker(browser, t, semaphore) for t in unique_sub_targets]
            await asyncio.gather(*tasks)
            return len(unique_sub_targets)
        except Exception as exc:
            logger.error("CareerViet global supervisor engine caught a logical fault: %s", exc, exc_info=True)
            return 0
        finally:
            await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone Test Entrypoint Executable for CareerViet.")
    parser.add_argument("--keyword", required=True)
    parser.add_argument("--site", default="careerviet.vn")
    parser.add_argument("--max-results", type=int, default=3)
    args = parser.parse_args()

    asyncio.run(run_crawl(args.keyword, args.site, args.max_results))


if __name__ == "__main__":
    main()