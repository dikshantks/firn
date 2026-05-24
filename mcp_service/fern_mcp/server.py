"""MCP server exposing safe Fern control-plane tools."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP


SENSITIVE_PROPERTY_PARTS = (
    "access-key",
    "access_key",
    "secret",
    "password",
    "session-token",
    "session_token",
    "token",
)


class FernClient:
    """Small REST client used by MCP tools."""

    def __init__(self) -> None:
        self.base_url = os.getenv("FERN_API_BASE_URL", "http://localhost:8000").rstrip("/")
        self.token = os.getenv("FERN_MCP_TOKEN")
        self.timeout = float(os.getenv("MCP_REQUEST_TIMEOUT_SECONDS", "30"))
        self.max_response_bytes = int(os.getenv("MCP_MAX_RESPONSE_BYTES", "250000"))

    def _headers(self) -> dict[str, str]:
        if not self.token:
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    def _redact(self, value: Any) -> Any:
        if isinstance(value, dict):
            redacted: dict[str, Any] = {}
            for key, nested in value.items():
                normalized = key.lower().replace(".", "-")
                if any(part in normalized for part in SENSITIVE_PROPERTY_PARTS):
                    redacted[key] = "***REDACTED***"
                else:
                    redacted[key] = self._redact(nested)
            return redacted
        if isinstance(value, list):
            return [self._redact(item) for item in value]
        return value

    def _cap(self, value: Any) -> Any:
        encoded = json.dumps(value, default=str)
        if len(encoded.encode("utf-8")) <= self.max_response_bytes:
            return value
        return {
            "truncated": True,
            "max_response_bytes": self.max_response_bytes,
            "message": "Response exceeded MCP_MAX_RESPONSE_BYTES; narrow the query.",
        }

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers(),
            timeout=self.timeout,
        ) as client:
            response = await client.request(method, path, params=params, json=json_body)
            response.raise_for_status()
            if not response.content:
                return {"ok": True}
            return self._cap(self._redact(response.json()))


client = FernClient()
mcp = FastMCP(
    "fern-control-plane",
    stateless_http=True,
    json_response=True,
    host=os.getenv("MCP_HOST", "0.0.0.0"),
    port=int(os.getenv("MCP_PORT", "8000")),
)


@mcp.tool()
async def fern_list_catalogs(include_stats: bool = False) -> Any:
    """List registered Fern catalogs."""
    return await client.request("GET", "/api/catalogs", params={"include_stats": include_stats})


@mcp.tool()
async def fern_get_catalog(name: str, include_stats: bool = False) -> Any:
    """Get sanitized catalog details."""
    return await client.request(
        "GET",
        f"/api/catalogs/{name}",
        params={"include_stats": include_stats},
    )


@mcp.tool()
async def fern_test_catalog(name: str) -> Any:
    """Test catalog connectivity."""
    return await client.request("GET", f"/api/catalogs/{name}/test")


@mcp.tool()
async def fern_list_namespaces(catalog: str) -> Any:
    """List namespaces in a catalog."""
    return await client.request("GET", "/api/tables/namespaces", params={"catalog": catalog})


@mcp.tool()
async def fern_list_tables(catalog: str, namespace: str | None = None) -> Any:
    """List tables in a catalog, optionally scoped to a namespace."""
    params: dict[str, Any] = {"catalog": catalog}
    if namespace:
        params["namespace"] = namespace
    return await client.request("GET", "/api/tables", params=params)


@mcp.tool()
async def fern_get_table_metadata(catalog: str, namespace: str, table: str) -> Any:
    """Get normalized Iceberg table metadata."""
    return await client.request(
        "GET",
        f"/api/tables/{namespace}/{table}",
        params={"catalog": catalog},
    )


@mcp.tool()
async def fern_get_catalog_health_summary(
    catalog: str,
    mode: str = "cached",
    max_cache_age_minutes: int = 60,
) -> Any:
    """Get catalog health summary."""
    return await client.request(
        "GET",
        "/api/health/summary",
        params={
            "catalog": catalog,
            "mode": mode,
            "max_cache_age_minutes": max_cache_age_minutes,
        },
    )


@mcp.tool()
async def fern_query_cached_table_health(
    catalog: str,
    status_filter: str | None = None,
    min_snapshots: int | None = None,
    min_delete_files: int | None = None,
    min_small_files: int | None = None,
    limit: int = 100,
    offset: int = 0,
) -> Any:
    """Query cached table health without scanning Glue/S3."""
    params: dict[str, Any] = {"catalog": catalog, "limit": limit, "offset": offset}
    for key, value in {
        "status_filter": status_filter,
        "min_snapshots": min_snapshots,
        "min_delete_files": min_delete_files,
        "min_small_files": min_small_files,
    }.items():
        if value is not None:
            params[key] = value
    return await client.request("GET", "/api/health/tables/cached", params=params)


@mcp.tool()
async def fern_get_table_health(catalog: str, namespace: str, table: str) -> Any:
    """Get detailed health assessment for a table."""
    return await client.request(
        "GET",
        f"/api/health/tables/{namespace}/{table}",
        params={"catalog": catalog},
    )


@mcp.tool()
async def fern_trigger_health_scan(catalog: str, mode: str = "light") -> Any:
    """Start a background health scan and return a job ID."""
    return await client.request(
        "POST",
        "/api/health/scan/trigger",
        params={"catalog": catalog, "mode": mode},
    )


@mcp.tool()
async def fern_get_job(job_id: str) -> Any:
    """Get a Fern background job."""
    return await client.request("GET", f"/api/jobs/{job_id}")


@mcp.tool()
async def fern_watch_job(
    job_id: str,
    poll_interval_seconds: float = 1.0,
    max_wait_seconds: float = 300.0,
) -> Any:
    """Poll a Fern job until completion, failure, or timeout."""
    deadline = asyncio.get_event_loop().time() + max_wait_seconds
    events: list[Any] = []

    while True:
        job = await client.request("GET", f"/api/jobs/{job_id}")
        events.append(job)
        if job.get("status") in {"completed", "failed"}:
            return {"events": events, "final": job}
        if asyncio.get_event_loop().time() >= deadline:
            return {"events": events, "timed_out": True, "final": job}
        await asyncio.sleep(poll_interval_seconds)


@mcp.tool()
async def fern_get_batch_optimization_plan(
    catalog: str,
    max_tables: int = 10,
    target_file_size_mb: int = 512,
    older_than_days: int = 30,
) -> Any:
    """Get a proposal-only optimization plan for multiple tables."""
    return await client.request(
        "GET",
        "/api/optimization/batch-optimization-plan",
        params={
            "catalog": catalog,
            "max_tables": max_tables,
            "target_file_size_mb": target_file_size_mb,
            "older_than_days": older_than_days,
        },
    )


@mcp.tool()
async def fern_get_table_optimization_plan(
    catalog: str,
    namespace: str,
    table: str,
    target_file_size_mb: int = 512,
    older_than_days: int = 30,
) -> Any:
    """Get a proposal-only optimization plan for one table."""
    return await client.request(
        "GET",
        f"/api/optimization/{namespace}/{table}/optimization-plan",
        params={
            "catalog": catalog,
            "target_file_size_mb": target_file_size_mb,
            "older_than_days": older_than_days,
        },
    )


@mcp.tool()
async def fern_get_spark_commands(
    catalog: str,
    namespace: str,
    table: str,
    language: str = "spark_sql",
    include_dry_run: bool = True,
) -> Any:
    """Get proposed Spark maintenance commands without executing them."""
    return await client.request(
        "GET",
        f"/api/optimization/{namespace}/{table}/spark-commands",
        params={
            "catalog": catalog,
            "language": language,
            "include_dry_run": include_dry_run,
        },
    )


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
