from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx


@dataclass
class ApiConfig:
    base_url: str
    timeout_seconds: int
    max_concurrency: int
    rate_limit_per_sec: int
    retry: Dict[str, Any]
    rolling_window_days: int = 7
    log_every_requests: int = 100
    games_chunk_days: int = 30
    lines_chunk_days: int = 30


class RateLimiter:
    def __init__(self, rate_per_sec: int) -> None:
        self.rate_per_sec = rate_per_sec
        self._lock = asyncio.Lock()
        self._tokens = rate_per_sec
        self._last = time.monotonic()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                refill = int(elapsed * self.rate_per_sec)
                if refill > 0:
                    self._tokens = min(self.rate_per_sec, self._tokens + refill)
                    self._last = now
                if self._tokens > 0:
                    self._tokens -= 1
                    return
            # Sleep outside the lock so other coroutines can proceed
            await asyncio.sleep(max(0.01, 1 / self.rate_per_sec))


class ApiClient:
    def __init__(self, token: str, cfg: ApiConfig) -> None:
        self.token = token
        self.cfg = cfg
        self._semaphore = asyncio.Semaphore(cfg.max_concurrency)
        self._limiter = RateLimiter(cfg.rate_limit_per_sec)
        self._client = httpx.AsyncClient(
            base_url=cfg.base_url,
            timeout=cfg.timeout_seconds,
            headers={"Authorization": f"Bearer {token}"},
        )
        self._logger = None

    def set_logger(self, logger) -> None:
        self._logger = logger

    async def close(self) -> None:
        await self._client.aclose()

    async def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        attempt = 0
        max_attempts = self.cfg.retry.get("max_attempts", 5)
        base_delay = self.cfg.retry.get("base_delay_seconds", 0.5)
        max_delay = self.cfg.retry.get("max_delay_seconds", 8)
        params = params or {}
        while True:
            attempt += 1
            async with self._semaphore:
                await self._limiter.acquire()
                if self._logger:
                    self._logger.info(
                        "http_request_start",
                        extra={"extra": {"path": path, "attempt": attempt, "params": params}},
                    )
                try:
                    resp = await self._client.get(path, params=params)
                except httpx.TimeoutException:
                    if self._logger:
                        self._logger.info("http_timeout", extra={"extra": {"path": path, "attempt": attempt}})
                    if attempt >= max_attempts:
                        raise
                    await asyncio.sleep(min(max_delay, base_delay * (2 ** (attempt - 1))))
                    continue
                except httpx.RequestError as exc:
                    if self._logger:
                        self._logger.info("http_error", extra={"extra": {"path": path, "attempt": attempt, "error": str(exc)}})
                    if attempt >= max_attempts:
                        raise
                    await asyncio.sleep(min(max_delay, base_delay * (2 ** (attempt - 1))))
                    continue
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_attempts:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    delay = min(max_delay, float(retry_after))
                else:
                    delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                if self._logger:
                    self._logger.info(
                        "http_retry",
                        extra={
                            "extra": {
                                "path": path,
                                "status": resp.status_code,
                                "attempt": attempt,
                                "delay": delay,
                            }
                        },
                    )
                await asyncio.sleep(delay)
                continue
            resp.raise_for_status()
