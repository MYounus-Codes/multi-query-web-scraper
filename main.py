"""
serp_scraper.py

An async, scalable Google Search scraper powered by SerpAPI.
- Takes a query (or multiple) and fetches results from Google via SerpAPI.
- Extracts: url, title, description/snippet, content_type, date (when present), and raw blocks.
- Streams results to JSON Lines (one object per line) so it scales and survives interruptions.
- Targets 500+ URLs/hour with adjustable concurrency & rate limit.
- Clean architecture, robust error handling, and easy to extend.

Usage (CLI):
    export SERPAPI_API_KEY=your_key_here
    python serp_scraper.py \
        --query "latest AI news" \
        --pages 6 \
        --per-page 20 \
        --outfile results.jsonl \
        --hl en --gl us \
        --concurrency 8 \
        --rpm 120

Notes:
- 500+ URLs/hour: With per-page=20 and pages=6 you already pull ~120 results in ~1 minute when rpm=120 and concurrency~8 (subject to plan limits). Tune rpm/concurrency to your account.
- JSONL is append-only. Safe to resume (script skips duplicates by id).
- You can also pass --queries-file queries.txt to scrape many queries in one run.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import httpx

load_dotenv()

print("DEBUG: Loaded API key =", os.getenv("SERPAPI_API_KEY"))


# ---------------------------
# Configuration & Data Models
# ---------------------------

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"

@dataclass
class SearchTask:
    query: str
    page: int  # 1-based page index
    per_page: int = 10  # SerpAPI: 10..100 typically supported
    hl: str = "en"
    gl: str = "us"
    location: Optional[str] = None

    def to_params(self) -> Dict[str, Any]:
        # Google uses 'start' offset in steps of 10; SerpAPI maps it too.
        # For per_page up to 100, start = (page-1)*per_page
        start = (self.page - 1) * self.per_page
        params: Dict[str, Any] = {
            "engine": "google",
            "q": self.query,
            "num": self.per_page,
            "start": start,
            "hl": self.hl,
            "gl": self.gl,
        }
        if self.location:
            params["location"] = self.location
        return params

# ---------------------------
# Utility: Rate Limiter (simple token bucket)
# ---------------------------

class RateLimiter:
    """Simple rate limiter: max `rpm` requests per minute across tasks."""

    def __init__(self, rpm: int):
        self.rpm = max(1, rpm)
        self.interval = 60.0 / self.rpm
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait_for = self._last + self.interval - now
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last = time.monotonic()

# ---------------------------
# SerpAPI Client
# ---------------------------

class SerpApiClient:
    def __init__(self, api_key: Optional[str] = None, timeout: float = 30.0):
        self.api_key = api_key or os.getenv("SERPAPI_API_KEY") or os.getenv("SERPAPI_KEY")
        if not self.api_key:
            raise RuntimeError("SERPAPI_API_KEY env var not set.")
        self.timeout = timeout
        self.client = httpx.AsyncClient(timeout=timeout)

    async def close(self):
        await self.client.aclose()

    async def search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # Merge api_key; SerpAPI requires it every call
        query = dict(params)
        query["api_key"] = self.api_key

        # Robust retries with exponential backoff on transient errors
        backoff = 1.0
        for attempt in range(6):
            try:
                r = await self.client.get(SERPAPI_ENDPOINT, params=query)
                if r.status_code == 200:
                    return r.json()
                elif r.status_code in {402, 403}:
                    # 402: payment required / plan limit; 403: forbidden
                    raise RuntimeError(
                        f"SerpAPI access issue: HTTP {r.status_code} — {r.text[:200]}"
                    )
                elif r.status_code in {429, 500, 502, 503, 504}:
                    # transient: backoff and retry
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue
                else:
                    # Other client or server errors
                    raise RuntimeError(
                        f"Unexpected SerpAPI response: {r.status_code} — {r.text[:200]}"
                    )
            except (httpx.TimeoutException, httpx.TransportError) as e:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                if attempt == 5:
                    raise RuntimeError(f"Network error after retries: {e}")
        raise RuntimeError("Exceeded retry attempts for SerpAPI request")

# ---------------------------
# Normalization / Extraction
# ---------------------------

def _infer_content_type(block_key: str, item: Dict[str, Any]) -> str:
    # Map SerpAPI blocks to a simple content_type
    mapping = {
        "organic_results": "organic",
        "news_results": "news",
        "videos_results": "video",
        "shopping_results": "shopping",
        "images_results": "image",
        "top_stories": "news",
        "answer_box": "answer",
        "knowledge_graph": "knowledge",
        "inline_videos": "video",
        "inline_images": "image",
    }
    return mapping.get(block_key, item.get("type") or block_key)


def _pick(item: Dict[str, Any], *keys: str) -> Optional[str]:
    for k in keys:
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return None


def normalize_result(block_key: str, item: Dict[str, Any], query: str, page: int, rank: int) -> Dict[str, Any]:
    url = _pick(item, "link", "url", "source")
    title = _pick(item, "title", "name")
    snippet = _pick(item, "snippet", "description")
    date = _pick(item, "date", "published", "published_at")

    content_type = _infer_content_type(block_key, item)

    return {
        "id": item.get("position")
        and f"{query}|p{page}|{block_key}|pos{item.get('position')}"
        or f"{query}|p{page}|{block_key}|{rank}",
        "query": query,
        "page": page,
        "rank": rank,
        "block": block_key,
        "content_type": content_type,
        "title": title,
        "url": url,
        "description": snippet,
        "date": date,
        "displayed_link": item.get("displayed_link"),
        "source": item.get("source"),
        "position": item.get("position"),
        "thumbnail": item.get("thumbnail") or item.get("image"),
        "raw": item,  # keep full raw for completeness
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


# ---------------------------
# Writer: JSON Lines with de-dup
# ---------------------------

class JsonlWriter:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._seen: set[str] = set()
        if self.path.exists():
            # load ids to avoid duplicates if resuming
            try:
                with self.path.open("r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            obj = json.loads(line)
                            _id = obj.get("id")
                            if isinstance(_id, str):
                                self._seen.add(_id)
                        except json.JSONDecodeError:
                            continue
            except Exception:
                pass

    def write(self, obj: Dict[str, Any]) -> None:
        _id = obj.get("id") or str(uuid.uuid4())
        if _id in self._seen:
            return
        self._seen.add(_id)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# ---------------------------
# Orchestrator
# ---------------------------

class Scraper:
    def __init__(self, api: SerpApiClient, outfile: Path, concurrency: int = 8, rpm: int = 120):
        self.api = api
        self.writer = JsonlWriter(outfile)
        self.semaphore = asyncio.Semaphore(max(1, concurrency))
        self.ratelimiter = RateLimiter(rpm)

    async def _fetch_page(self, task: SearchTask) -> List[Dict[str, Any]]:
        await self.ratelimiter.wait()
        params = task.to_params()
        data = await self.api.search(params)

        results: List[Dict[str, Any]] = []
        rank = 0
        # Iterate over known blocks. Extend as needed.
        for block in (
            "organic_results",
            "top_stories",
            "news_results",
            "videos_results",
            "inline_videos",
            "images_results",
            "inline_images",
            "shopping_results",
            "answer_box",
            "knowledge_graph",
        ):
            items = data.get(block)
            if isinstance(items, list):
                for item in items:
                    rank += 1
                    results.append(normalize_result(block, item, task.query, task.page, rank))
            elif isinstance(items, dict):
                # Single-result blocks like answer_box/knowledge_graph
                rank += 1
                results.append(normalize_result(block, items, task.query, task.page, rank))

        return results

    async def _run_task(self, task: SearchTask) -> int:
        async with self.semaphore:
            try:
                page_results = await self._fetch_page(task)
                for obj in page_results:
                    self.writer.write(obj)
                return len(page_results)
            except Exception as e:
                # Log error neatly to stderr, continue other tasks
                sys.stderr.write(f"[ERROR] {task.query} p{task.page}: {e}\n")
                return 0

    async def run(self, queries: List[str], pages: int, per_page: int, hl: str, gl: str, location: Optional[str] = None) -> int:
        tasks: List[asyncio.Task] = []
        for q in queries:
            for p in range(1, pages + 1):
                t = SearchTask(query=q, page=p, per_page=per_page, hl=hl, gl=gl, location=location)
                tasks.append(asyncio.create_task(self._run_task(t)))
        counts = await asyncio.gather(*tasks)
        return sum(counts)

# ---------------------------
# CLI
# ---------------------------

def parse_args(argv: List[str]) -> Any:
    import argparse

    ap = argparse.ArgumentParser(description="SerpAPI Google Search scraper → JSONL")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--query", type=str, help="Single query string")
    g.add_argument("--queries-file", type=str, help="Path to file with one query per line")

    ap.add_argument("--pages", type=int, default=3, help="Pages per query (1-based). Use with --per-page.")
    ap.add_argument("--per-page", type=int, default=20, help="Results per page (10..100 typical)")
    ap.add_argument("--outfile", type=str, default="results.jsonl", help="Path to JSONL output file")
    ap.add_argument("--hl", type=str, default="en", help="Interface language (hl)")
    ap.add_argument("--gl", type=str, default="us", help="Geo (gl)")
    ap.add_argument("--location", type=str, default=None, help="Location string (optional)")
    ap.add_argument("--concurrency", type=int, default=8, help="Max concurrent page requests")
    ap.add_argument("--rpm", type=int, default=120, help="Global rate limit (requests per minute)")

    return ap.parse_args(argv)


async def amain(argv: List[str]) -> None:
    args = parse_args(argv)

    if args.queries_file:
        with open(args.queries_file, "r", encoding="utf-8") as f:
            queries = [ln.strip() for ln in f if ln.strip()]
    else:
        queries = [args.query]

    client = SerpApiClient()
    scraper = Scraper(api=client, outfile=Path(args.outfile), concurrency=args.concurrency, rpm=args.rpm)

    try:
        total = await scraper.run(
            queries=queries,
            pages=args.pages,
            per_page=args.per_page,
            hl=args.hl,
            gl=args.gl,
            location=args.location,
        )
        print(f"Saved {total} result objects to {args.outfile}")
    finally:
        await client.close()


def main() -> None:
    try:
        asyncio.run(amain(sys.argv[1:]))
    except KeyboardInterrupt:
        print("Interrupted.")


if __name__ == "__main__":
    main()
