"""
Single shared async Redis connection pool for the whole worker process.

cache.py and store.py used to each call aioredis.from_url() independently, i.e.
two separate, effectively unbounded connection pools. Under heavy concurrency
(hundreds of in-flight reports) that can open thousands of sockets to Redis and
exhaust file descriptors. One shared, *bounded* pool keeps the connection count
predictable and is shared by both the API cache (cache.py) and the job/result
stores (store.py).

We use a BlockingConnectionPool so that when every connection is busy a caller
waits briefly for a free one (Redis ops are sub-millisecond) instead of erroring
out — which, in cache.py's try/except-as-cache-miss flow, would otherwise turn
into an unnecessary upstream API hit.
"""

import redis.asyncio as aioredis

from config import REDIS_URL, REDIS_MAX_CONNECTIONS

_pool = aioredis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=False,
    max_connections=REDIS_MAX_CONNECTIONS,
    timeout=20,  # seconds a caller waits for a free connection before raising
)

# Import this everywhere instead of creating new clients.
redis_client: aioredis.Redis = aioredis.Redis(connection_pool=_pool)
