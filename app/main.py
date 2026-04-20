#!/usr/bin/env python3
"""
main.py — MemPalace MCP Server (Databricks App entrypoint).

Exposes all 19 MemPalace tools via the Model Context Protocol over
streamable HTTP.  Deployed as a Databricks App so any MCP-compatible
agent can call it.

The root-level ``app.yaml`` launches this via::

    uvicorn app.main:app --host 0.0.0.0 --port 8000

Because uvicorn runs from the project root, ``mempalace.*`` is
importable without any sys.path manipulation.
"""

import logging
import sys
import time
from pathlib import Path

# Safety net: if someone runs ``python app/main.py`` directly, the
# project root may not be on sys.path.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from mcp.server.fastmcp import FastMCP  # noqa: E402
from mcp.server.transport_security import TransportSecuritySettings  # noqa: E402
from starlette.middleware import Middleware  # noqa: E402
from starlette.middleware.cors import CORSMiddleware  # noqa: E402
from starlette.requests import Request  # noqa: E402
from starlette.responses import Response  # noqa: E402
from starlette.types import ASGIApp, Receive, Scope, Send  # noqa: E402

from mempalace.mcp_server import TOOLS  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("mempalace_app")


# ── Request timing middleware ─────────────────────────────────────────────────


class TimingMiddleware:
    """Log wall-clock latency of every ASGI request."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        t0 = time.perf_counter()
        path = scope.get("path", "?")

        async def timed_send(message):
            if message["type"] == "http.response.start":
                elapsed_ms = (time.perf_counter() - t0) * 1000
                status = message.get("status", "?")
                logger.info("REQ %s → %s in %.0fms", path, status, elapsed_ms)
            await send(message)

        await self.app(scope, receive, timed_send)


# ── Build FastMCP server ──────────────────────────────────────────────────────
# Disable DNS rebinding protection — the app is behind Databricks OAuth proxy
# which handles auth. Genie Code and AI Playground send cross-origin requests
# from the workspace domain that would be blocked by default.

mcp = FastMCP(
    "mempalace",
    stateless_http=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)


def _register_tools() -> None:
    """Wrap each handler from the TOOLS dict as a FastMCP tool."""
    for tool_name, spec in TOOLS.items():
        handler = spec["handler"]
        description = spec["description"]
        mcp.tool(name=tool_name, description=description)(handler)


_register_tools()

# ── ASGI app for Databricks App runtime ───────────────────────────────────────
# Genie Code and AI Playground make cross-origin requests to the app.
# CORS middleware is required for Databricks-hosted MCP clients.
# The app is already behind Databricks OAuth — allow all origins.

app = mcp.streamable_http_app()

app.add_middleware(TimingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Startup: pre-warm SQL warehouse connection ───────────────────────────────

_WARM_DONE = False


async def _lifespan_prewarm(app: ASGIApp):
    """Fire a lightweight SQL query on startup to warm the warehouse."""
    global _WARM_DONE
    if not _WARM_DONE:
        try:
            from mempalace.mcp_server import _sql
            logger.info("Pre-warming SQL warehouse connection...")
            t0 = time.perf_counter()
            _sql("SELECT 1")
            elapsed = (time.perf_counter() - t0) * 1000
            logger.info("SQL warehouse warm in %.0fms", elapsed)
        except Exception as exc:
            logger.warning("Pre-warm failed (non-fatal): %s", exc)
        _WARM_DONE = True
    yield


# Note: Starlette's lifespan requires passing at app construction time.
# Since mcp.streamable_http_app() doesn't expose lifespan, we pre-warm
# on first request instead via a one-shot middleware.


class PreWarmMiddleware:
    """One-shot middleware: pre-warms SQL warehouse on the first request."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        self._warmed = False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self._warmed and scope["type"] == "http":
            self._warmed = True
            try:
                import asyncio
                from mempalace.mcp_server import _sql

                loop = asyncio.get_event_loop()
                logger.info("First request — pre-warming SQL warehouse...")
                t0 = time.perf_counter()
                await loop.run_in_executor(None, _sql, "SELECT 1")
                elapsed = (time.perf_counter() - t0) * 1000
                logger.info("SQL warehouse warm in %.0fms", elapsed)
            except Exception as exc:
                logger.warning("Pre-warm failed (non-fatal): %s", exc)
        await self.app(scope, receive, send)


app.add_middleware(PreWarmMiddleware)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
