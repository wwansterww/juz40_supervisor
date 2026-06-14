"""
Shared helpers for routes that need paginated course loading.
"""
import asyncio
import httpx
from cache import api_get_async


async def fetch_all_course_pages(
    base_url_page0: str,
    token: str,
    client: httpx.AsyncClient,
) -> list:
    """
    Fetch all pages of a paginated courses endpoint.
    *base_url_page0* must already contain `page=0` as a query parameter.

    Returns the combined list of course objects from all pages.

    Raises if any page fails: a silently truncated course list is
    indistinguishable from a complete one — reports built from it would be
    quietly missing whole courses. Callers catch the error and show it.
    """
    first = await api_get_async(base_url_page0, token, client)
    content = list(first.get("content", []))
    total_pages = first.get("totalPages", 1)

    if total_pages <= 1:
        return content

    # Remaining pages in parallel — just swap page=0 → page=N
    rest_urls = [
        base_url_page0.replace("page=0", f"page={p}")
        for p in range(1, total_pages)
    ]
    results = await asyncio.gather(
        *[api_get_async(u, token, client) for u in rest_urls],
    )
    for r in results:
        content.extend(r.get("content", []))

    return content
