import logging
import httpx
from workers.schemas import CrawlTarget

logger = logging.getLogger(__name__)

def search_searxng(keyword: str, site: str | None, max_results: int = 3) -> list[CrawlTarget]:
    """
    Queries the local SearXNG container instance to retrieve search results safely.
    
    Swallows remote protocol and connection errors to prevent downstream pipeline crashes
    and allow the orchestrator to trigger its self-healing retry mechanisms.
    
    Args:
        keyword: The unified lookup search term (e.g., 'ai engineer').
        site: Domain boundary scope limit to restrict search results.
        max_results: Strict maximum result batch size ceiling cap metrics.
        
    Returns:
        A list of mapped CrawlTarget objects. Returns an empty list on network failures.
    """
    # Initialize an empty target storage array
    targets = []
    
    # Enforce exact phrasing using double quotes and append target site constraint if present
    query_str = f'"{keyword}"'
    if site:
        query_str += f" site:{site}"
        
    url = "http://localhost:8888/search"
    params = {
        "q": query_str,
        "format": "json",
        "pageno": 1
    }
    
    try:
        # Execute the synchronous network request with a strict timeout guardrail
        with httpx.Client(timeout=10.0) as client:
            response = client.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                # Slice and map raw JSON entries into structured schema objects
                for res in results[:max_results]:
                    targets.append(CrawlTarget(
                        url=res.get("url"),
                        query=keyword,
                        site=site or "unknown"
                    ))
                    
    # CRITICAL PROTECTION PATCH: Intercept SearXNG/upstream disconnection anomalies cleanly
    except (httpx.RemoteProtocolError, httpx.HTTPError) as net_exc:
        logger.warning(
            "SearXNG network or protocol disruption intercepted: %s. "
            "Returning empty cluster seed to trigger defensive orchestrator retry loop.", 
            net_exc
        )
        return [] # Return an empty list to seamlessly bubble up a recovery retry trigger
        
    except Exception as general_exc:
        logger.error(
            "Unexpected internal parsing anomaly diagnosed inside search engine abstraction: %s", 
            general_exc
        )
        return []
        
    return targets