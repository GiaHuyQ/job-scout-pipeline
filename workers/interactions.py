import logging
import random
import time
import asyncio
from playwright.async_api import Page

logger = logging.getLogger(__name__)

# Standard high-reputation User-Agent used across all browser sessions
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

JS_BOUNDED_SMOOTH_SCROLL = """
async (targetSelector) => {
    const target = document.querySelector(targetSelector);
    if (!target) return;

    await new Promise((resolve) => {
        const distance = 50; 
        const timer = setInterval(() => {
            const rect = target.getBoundingClientRect();
            const targetBottomAbsolute = rect.bottom + window.scrollY;
            const currentViewportBottom = window.scrollY + window.innerHeight;

            window.scrollBy(0, distance);

            if (currentViewportBottom >= targetBottomAbsolute || (window.scrollY + window.innerHeight) >= document.body.scrollHeight) {
                clearInterval(timer);
                resolve();
            }
        }, 60); 
    });
}
"""

JS_PURGE_STICKY_OVERLAYS = """
() => {
    const elements = document.querySelectorAll('*');
    for (const el of elements) {
        const computedStyle = window.getComputedStyle(el);
        if (computedStyle.position === 'fixed' || computedStyle.position === 'sticky') {
            el.style.setProperty('display', 'none', 'important');
        }
    }
}
"""


async def smooth_scroll_targeted_element(page: Page, selector: str) -> None:
    """Simulates incremental smooth scrolling bounded strictly inside an element container."""
    logger.debug("Executing shared element-bounded smooth scroll tracking.")
    await page.evaluate(JS_BOUNDED_SMOOTH_SCROLL, selector)
    await page.wait_for_timeout(1000)


async def purge_sticky_overlays(page: Page) -> None:
    """Destroys fixed/sticky overlay menus dynamically to block stitching artifacts."""
    logger.debug("Executing dynamic injection rule to purge fixed/sticky CSS components.")
    await page.evaluate(JS_PURGE_STICKY_OVERLAYS)
    await page.wait_for_timeout(300)


async def human_like_idle_scrolling(page: Page, duration: float) -> None:
    """
    Simulates a human idle reading behavior by randomly scrolling slightly up and down
    using native hardware mouse wheel events before closing the page.
    """
    logger.info("Simulating human idle reading/hover behavior for %.2f seconds...", duration)
    start_time = time.time()
    
    while time.time() - start_time < duration:
        # 70% chance to scroll down slightly, 30% chance to scroll back up
        direction = random.choice([1, 1, 1, -1])
        # Random micro-pixels amount mimicking real finger trackpad/wheel flicks
        scroll_amount = direction * random.randint(90, 120)
        
        # Emits absolute hardware level scroll event (Highly stealthy)
        await page.mouse.wheel(delta_x=0, delta_y=scroll_amount)
        
        # Random breathing pause between each micro-movement (300ms to 700ms)
        await asyncio.sleep(random.uniform(0.3, 0.7))