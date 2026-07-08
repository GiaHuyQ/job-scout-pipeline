import logging
import httpx
from workers.schemas import CrawlTarget

logger = logging.getLogger(__name__)

def search_searxng(keyword: str, site: str | None, max_results: int = 3) -> list[CrawlTarget]:
    """
    Search for jobs using a local SearXNG instance.
    
    It tries search engines one by one in this order:
    bing -> duckduckgo -> brave -> google.
    
    It returns results from the first engine that works successfully.
    This helps to avoid getting blocked by making too many requests.

    Args:
        keyword: The job title or skill to search (e.g., 'python developer').
        site: The target domain to restrict results (e.g., 'itviec.com').
        max_results: The maximum number of links to return.

    Returns:
        A list of CrawlTarget objects, or an empty list if all engines fail.
    """
    # 1. Create the exact query string
    query_str = f'"{keyword}"'
    if site:
        query_str += f" site:{site}"
        
    url = "http://localhost:8888/search"
    
    # 2. Define the fallback order of search engines
    engines_order = ["bing", "duckduckgo", "brave", "yahoo", "google"]
    
    # Use a single HTTP client session to reuse connections efficiently
    with httpx.Client(timeout=10.0) as client:
        for engine in engines_order:
            # Configure params to force SearXNG to use only ONE specific engine
            params = {
                "q": query_str,
                "format": "json",
                "pageno": 1,
                "engines": engine  # Instruct SearXNG to query this engine only
            }
            
            logger.info("Attempting search with engine: [%s] for query: %s", engine, query_str)
            
            try:
                response = client.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    results = data.get("results", [])
                    
                    # EARLY RETURN: If this engine has results, parse and return them immediately!
                    if results:
                        logger.info("Successfully fetched results from engine: [%s]", engine)
                        targets = []
                        for res in results[:max_results]:
                            targets.append(CrawlTarget(
                                url=res.get("url"),
                                query=keyword,
                                site=site or "unknown"
                            ))
                        return targets # Stop the loop and return the data safely
                    
                    # If status is 200 but results list is empty, log it and try next engine
                    logger.warning("Engine [%s] returned 0 results. Moving to next fallback.", engine)
                    
            # Handle specific network/protocol errors for the current engine
            except (httpx.RemoteProtocolError, httpx.HTTPError) as net_exc:
                logger.warning("Network issue with engine [%s]: %s. Trying next fallback.", engine, net_exc)
                continue # Skip to the next engine in the list
                
            # Handle any other unexpected errors safely
            except Exception as general_exc:
                logger.error("Unexpected error occurred while parsing engine [%s]: %s", engine, general_exc)
                continue # Keep the pipeline alive and try the next engine

    # 3. If the loop completes and no engine returned results, return an empty list
    logger.error("All search engines (bing, duckduckgo, brave, yahoo, google) failed to return results.")
    return []