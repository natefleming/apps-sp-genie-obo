"""Minimal LangGraph agent whose single Genie tool runs as the *calling* principal.

The Databricks Apps proxy strips the caller's ``Authorization`` header and injects
``x-forwarded-access-token`` (an OBO token scoped by the app's ``user_api_scopes``).
The MLflow Agent Server stores the request headers in a ContextVar, so the tool can
build a WorkspaceClient that acts as the caller — a user or a service principal.
The LLM itself is called as the App's own service principal.
"""

from __future__ import annotations

import os
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks_langchain import ChatDatabricks
from databricks_openai import AsyncDatabricksOpenAI
from langchain.agents import create_agent
from langchain_core.tools import tool
from langgraph.graph.state import CompiledStateGraph
from mlflow.genai.agent_server.utils import get_request_headers

SYSTEM_PROMPT: str = (
    "You answer questions about restaurant reservations. Always use the ask_genie tool "
    "to get data, and only report what it returns. If it returns no rows, say so."
)


def _host() -> str:
    # Databricks Apps inject DATABRICKS_HOST as a bare hostname.
    host: str = os.environ["DATABRICKS_HOST"].rstrip("/")
    return host if host.startswith("http") else f"https://{host}"


def caller_workspace_client() -> WorkspaceClient:
    """WorkspaceClient acting as the caller, built from the forwarded OBO token.

    Fails loudly when the token is missing: silently falling back to the App SP
    would bypass the caller's row-level security.
    """
    token: str | None = get_request_headers().get("x-forwarded-access-token")
    if not token:
        raise PermissionError(
            "No x-forwarded-access-token on the request; refusing to query Genie as the App SP."
        )
    # Explicit pat config avoids clashing with the App SP's ambient client id/secret env vars.
    return WorkspaceClient(config=Config(host=_host(), token=token, auth_type="pat"))


@tool
async def ask_genie(question: str) -> str:
    """Ask the reservations Genie agent a natural-language data question."""
    client = AsyncDatabricksOpenAI(
        workspace_client=caller_workspace_client(),
        base_url=f"{_host()}/api/2.0/genie/agents/{os.environ['GENIE_SPACE_ID']}",
    )
    stream = await client.responses.create(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": question}],
            }
        ],
        stream=True,
        model="genie",
    )
    answer_parts: list[str] = []
    async for event in stream:
        if getattr(event, "type", "") != "response.output_item.done":
            continue
        item: dict[str, Any] = event.item.model_dump()
        if item.get("type") == "message":
            answer_parts.extend(
                block.get("text", "") for block in item.get("content") or [] if "text" in block
            )
    return "".join(answer_parts) or "(Genie returned no text.)"


def build_agent() -> CompiledStateGraph:
    """Build the single-tool agent (LLM runs as the App service principal)."""
    # Claude Sonnet 5 rejects the temperature parameter, so none is set.
    model = ChatDatabricks(endpoint=os.environ["LLM_ENDPOINT"])
    return create_agent(model=model, tools=[ask_genie], system_prompt=SYSTEM_PROMPT)
