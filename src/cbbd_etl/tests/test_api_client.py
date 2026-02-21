"""Tests for the async API client using httpx.MockTransport."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from cbbd_etl.api_client import ApiClient, ApiConfig, RateLimiter


def _make_api_config(**overrides) -> ApiConfig:
    """Create a minimal ApiConfig for testing."""
    defaults = {
        "base_url": "https://api.test.com",
        "timeout_seconds": 5,
        "max_concurrency": 3,
        "rate_limit_per_sec": 100,  # High rate limit to avoid slowness in tests
        "retry": {
            "max_attempts": 3,
            "base_delay_seconds": 0.001,
            "max_delay_seconds": 0.01,
        },
    }
    defaults.update(overrides)
    return ApiConfig(**defaults)


class TestGetJsonSuccess:
    async def test_get_json_success(self):
        """Mock a 200 response, verify JSON returned."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(
                200,
                json={"data": [{"id": 1}]},
            )

        cfg = _make_api_config()
        client = ApiClient("test-token", cfg)
        # Replace the internal httpx client with a mock transport
        await client._client.aclose()
        client._client = httpx.AsyncClient(
            base_url=cfg.base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            result = await client.get_json("/games", params={"season": 2024})
            assert result == {"data": [{"id": 1}]}
            assert call_count == 1
        finally:
            await client.close()


class TestGetJsonRetryOn429:
    async def test_get_json_retry_on_429(self):
        """Mock 429 then 200, verify retry succeeds."""
        attempt = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                return httpx.Response(429, json={"error": "rate limited"})
            return httpx.Response(200, json={"data": "ok"})

        cfg = _make_api_config()
        client = ApiClient("test-token", cfg)
        await client._client.aclose()
        client._client = httpx.AsyncClient(
            base_url=cfg.base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            result = await client.get_json("/games")
            assert result == {"data": "ok"}
            assert attempt == 2
        finally:
            await client.close()


class TestGetJsonRetryOn500:
    async def test_get_json_retry_on_500(self):
        """Mock 500 then 200, verify retry succeeds."""
        attempt = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                return httpx.Response(500, json={"error": "server error"})
            return httpx.Response(200, json={"data": "recovered"})

        cfg = _make_api_config()
        client = ApiClient("test-token", cfg)
        await client._client.aclose()
        client._client = httpx.AsyncClient(
            base_url=cfg.base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            result = await client.get_json("/games")
            assert result == {"data": "recovered"}
            assert attempt == 2
        finally:
            await client.close()


class TestGetJsonTimeoutRetry:
    async def test_get_json_timeout_retry(self):
        """Mock timeout then 200, verify retry."""
        attempt = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise httpx.ReadTimeout("Connection timed out")
            return httpx.Response(200, json={"data": "after_timeout"})

        cfg = _make_api_config()
        client = ApiClient("test-token", cfg)
        await client._client.aclose()
        client._client = httpx.AsyncClient(
            base_url=cfg.base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            result = await client.get_json("/games")
            assert result == {"data": "after_timeout"}
            assert attempt == 2
        finally:
            await client.close()


class TestGetJsonMaxRetriesExhausted:
    async def test_get_json_max_retries_exhausted(self):
        """Mock all failures, verify raises."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={"error": "always failing"})

        cfg = _make_api_config()
        client = ApiClient("test-token", cfg)
        await client._client.aclose()
        client._client = httpx.AsyncClient(
            base_url=cfg.base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            with pytest.raises(httpx.HTTPStatusError):
                await client.get_json("/games")
        finally:
            await client.close()


class TestRateLimiter:
    async def test_rate_limiter_acquire(self):
        """Verify token consumption behavior."""
        limiter = RateLimiter(rate_per_sec=100)

        # Should be able to acquire several tokens quickly
        for _ in range(3):
            await limiter.acquire()

        # Tokens should have been consumed
        assert limiter._tokens < 100

    async def test_rate_limiter_initial_tokens(self):
        """Rate limiter starts with rate_per_sec tokens."""
        limiter = RateLimiter(rate_per_sec=50)
        assert limiter._tokens == 50

        await limiter.acquire()
        assert limiter._tokens == 49
