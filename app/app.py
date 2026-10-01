"""MLflow Agent Server entrypoint for Databricks Apps.

Exposes ``/responses`` (OpenAI Responses contract) plus ``/whoami``, a debug route
that shows which identity the Apps proxy forwarded for the caller.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, AsyncGenerator
from uuid import uuid4

import mlflow
import uvicorn
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from fastapi import Request
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from mlflow.genai.agent_server import AgentServer, invoke, stream
from mlflow.genai.agent_server.utils import get_request_headers
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    create_function_call_item,
    create_function_call_output_item,
    create_text_output_item,
    to_chat_completions_input,
)

from agent import _host, build_agent, caller_workspace_client

mlflow.langchain.autolog()

_graph = build_agent()

agent_server = AgentServer("ResponsesAgent", enable_chat_proxy=False)
app = agent_server.app


def _answer_text(message: AIMessage) -> str:
    """Visible answer text only.

    ChatDatabricks JSON-serializes list content (e.g. Claude reasoning + text blocks)
    into a string, so parse it back and keep just the text blocks.
    """
    text: str = message.text
    if text.startswith("[{"):
        try:
            blocks: list[dict[str, Any]] = json.loads(text)
        except json.JSONDecodeError:
            return text
        return "".join(b.get("text", "") for b in blocks if isinstance(b, dict) and b.get("type") == "text")
    return text


def _to_items(message: BaseMessage) -> list[dict[str, Any]]:
    """Convert one LangChain message into Responses output items."""
    if isinstance(message, AIMessage):
        items: list[dict[str, Any]] = []
        if text := _answer_text(message):
            items.append(create_text_output_item(text=text, id=message.id or str(uuid4())))
        for call in message.tool_calls:
            items.append(
                create_function_call_item(
                    id=call["id"], call_id=call["id"], name=call["name"], arguments=str(call["args"])
                )
            )
        return items
    if isinstance(message, ToolMessage):
        return [create_function_call_output_item(call_id=message.tool_call_id, output=message.text)]
    return []


async def _identity() -> dict[str, str]:
    """Who called the app (per the proxy) and who Genie will see (per the OBO token)."""
    me = await asyncio.to_thread(lambda: caller_workspace_client().current_user.me())
    return {
        "caller": get_request_headers().get("x-forwarded-email", ""),
        "genie_identity": me.user_name or "",
        "genie_identity_display_name": me.display_name or "",
    }


@stream()
async def streaming(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    inputs = {"messages": to_chat_completions_input([i.model_dump() for i in request.input])}
    async for update in _graph.astream(inputs, stream_mode="updates"):
        for node_output in update.values():
            for message in (node_output or {}).get("messages", []):
                for item in _to_items(message):
                    yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)


@invoke()
async def non_streaming(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    output = [event.item async for event in streaming(request)]
    return ResponsesAgentResponse(output=output, custom_outputs=await _identity())


@app.get("/whoami")
async def whoami(request: Request) -> dict[str, Any]:
    """Debug: what the Apps proxy forwarded, and who that token resolves to."""
    token: str | None = request.headers.get("x-forwarded-access-token")
    resolved: str | None = None
    if token:
        client = WorkspaceClient(config=Config(host=_host(), token=token, auth_type="pat"))
        resolved = (await asyncio.to_thread(client.current_user.me)).user_name
    return {
        "x_forwarded_email": request.headers.get("x-forwarded-email"),
        "x_forwarded_preferred_username": request.headers.get("x-forwarded-preferred-username"),
        "has_forwarded_access_token": token is not None,
        "token_resolves_to": resolved,
        "app_service_principal": os.environ.get("DATABRICKS_CLIENT_ID"),
    }


def main() -> None:
    # Databricks Apps assign the port; binding anything else 502s behind the proxy.
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))


if __name__ == "__main__":
    main()
