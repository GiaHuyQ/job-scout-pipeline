import logging
from urllib.parse import urlparse

import httpx

from workers.schemas import CrawlTarget


logger = logging.getLogger(__name__)


def is_target_domain(url: str, site: str) -> bool:
    """
    Check whether a URL belongs to the requested target domain.

    Examples:
        careerviet.vn                   -> True
        www.careerviet.vn               -> True
        jobs.careerviet.vn              -> True
        https://careerviet.vn/job/123   -> True
        https://www.topcv.vn/...        -> False
        https://google.com/...          -> False
    """
    try:
        hostname = urlparse(url).hostname

        if not hostname:
            return False

        hostname = hostname.lower().removeprefix("www.")
        target_domain = site.lower().removeprefix("www.")

        return (
            hostname == target_domain
            or hostname.endswith(f".{target_domain}")
        )

    except Exception as exc:
        logger.warning("Failed to validate URL domain [%s]: %s", url, exc)
        return False


def search_searxng(
    keyword: str,
    site: str | None,
    max_results: int = 3,
) -> list[CrawlTarget]:
    """
    Search for jobs using a local SearXNG instance.

    It tries search engines in this order:
        bing -> duckduckgo -> brave -> yahoo -> google

    Results are filtered so that only URLs belonging to `site`
    are returned.
    """

    # ------------------------------------------------------------------
    # 1. Build search query
    # ------------------------------------------------------------------
    query_str = f'"{keyword}"'

    normalized_site = None
    if site:
        normalized_site = site.lower().removeprefix("www.")
        query_str += f" site:{normalized_site}"

    url = "http://localhost:8888/search"

    # ------------------------------------------------------------------
    # 2. Fallback search engine order
    # ------------------------------------------------------------------
    engines_order = [
        "bing",
        "duckduckgo",
        "brave",
        "yahoo",
        "google",
        "yandex"
    ]

    # ------------------------------------------------------------------
    # 3. Reuse one HTTP connection
    # ------------------------------------------------------------------
    with httpx.Client(timeout=10.0) as client:
        for engine in engines_order:
            params = {
                "q": query_str,
                "format": "json",
                "pageno": 1,
                "engines": engine,
            }

            logger.info(
                "Attempting search with engine: [%s] for query: %s",
                engine,
                query_str,
            )

            try:
                response = client.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                results = data.get("results", [])

                if not results:
                    logger.warning(
                        "Engine [%s] returned 0 results. "
                        "Moving to next fallback.",
                        engine,
                    )
                    continue

                logger.info(
                    "Engine [%s] returned %d raw results.",
                    engine,
                    len(results),
                )

                # ------------------------------------------------------
                # 4. Filter by target domain
                # ------------------------------------------------------
                targets: list[CrawlTarget] = []

                for res in results:
                    result_url = res.get("url")

                    if not result_url:
                        logger.warning(
                            "Engine [%s] returned a result without URL. "
                            "Skipping.",
                            engine,
                        )
                        continue

                    # If a target site is specified, enforce domain match.
                    if normalized_site and not is_target_domain(
                        result_url,
                        normalized_site,
                    ):
                        logger.warning(
                            "Rejecting result outside target domain [%s]: %s",
                            normalized_site,
                            result_url,
                        )
                        continue

                    targets.append(
                        CrawlTarget(
                            url=result_url,
                            query=keyword,
                            site=normalized_site or "unknown",
                        )
                    )

                    if len(targets) >= max_results:
                        break

                # ------------------------------------------------------
                # 5. Only return if valid domain results exist
                # ------------------------------------------------------
                if targets:
                    logger.info(
                        "Successfully fetched %d valid results "
                        "from engine: [%s] for domain [%s].",
                        len(targets),
                        engine,
                        normalized_site or "any",
                    )
                    return targets

                # Engine had results, but none matched requested domain.
                logger.warning(
                    "Engine [%s] returned results, but none matched "
                    "target domain [%s]. Trying next fallback.",
                    engine,
                    normalized_site or "any",
                )

            except (httpx.RemoteProtocolError, httpx.HTTPError) as net_exc:
                logger.warning(
                    "Network issue with engine [%s]: %s. "
                    "Trying next fallback.",
                    engine,
                    net_exc,
                )
                continue

            except Exception as general_exc:
                logger.error(
                    "Unexpected error occurred while parsing "
                    "engine [%s]: %s",
                    engine,
                    general_exc,
                    exc_info=True,
                )
                continue

    # ------------------------------------------------------------------
    # 6. All engines exhausted
    # ------------------------------------------------------------------
    logger.error(
        "All search engines failed to return valid results "
        "for domain [%s].",
        normalized_site or "any",
    )

    return []