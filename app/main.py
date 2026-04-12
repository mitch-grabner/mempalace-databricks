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

import sys
from pathlib import Path

# Safety net: if someone runs ``python app/main.py`` directly, the
# project root may not be on sys.path.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from mcp.server.fastmcp import FastMCP  # noqa: E402
from mcp.server.transport_security import TransportSecuritySettings  # noqa: E402
from starlette.middleware.cors import CORSMiddleware  # noqa: E402

from mempalace.mcp_server import TOOLS  # noqa: E402

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
