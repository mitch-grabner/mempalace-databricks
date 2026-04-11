#!/usr/bin/env python3
"""
main.py — MemPalace MCP Server (Databricks App entrypoint).

Exposes all 19 MemPalace tools via the Model Context Protocol over
streamable HTTP.  Deployed as a Databricks App so any MCP-compatible
agent can call it.

Usage:
    Deployed via ``databricks apps deploy`` — the command in ``app.yaml``
    runs this file.  For local testing:

        MEMPALACE_WAREHOUSE_ID=<wh_id> python app/main.py
"""

import sys
from pathlib import Path

# Ensure the project root is importable so ``mempalace.*`` resolves.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from mcp.server.fastmcp import FastMCP  # noqa: E402

from mempalace.mcp_server import TOOLS  # noqa: E402

# ── Build FastMCP server ──────────────────────────────────────────────────────

mcp = FastMCP(
    "mempalace",
    stateless_http=True,
)


def _register_tools() -> None:
    """Wrap each handler from the TOOLS dict as a FastMCP tool."""
    for tool_name, spec in TOOLS.items():
        handler = spec["handler"]
        description = spec["description"]
        # FastMCP's @mcp.tool() can also be called imperatively:
        mcp.tool(name=tool_name, description=description)(handler)


_register_tools()

# ── ASGI app for Databricks App runtime ───────────────────────────────────────
# Databricks Apps expect an HTTP server.  FastMCP exposes a Starlette ASGI app
# via ``streamable_http_app()`` which handles the MCP Streamable HTTP transport.

app = mcp.streamable_http_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
    )
