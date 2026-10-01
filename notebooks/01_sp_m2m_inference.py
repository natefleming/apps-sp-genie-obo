# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Call a Databricks App as a service principal (OAuth M2M) and prove Genie runs as that SP
# MAGIC
# MAGIC This is the Apps equivalent of today's Model Serving flow:
# MAGIC
# MAGIC | Step | Model Serving (today) | Databricks Apps (this demo) |
# MAGIC |---|---|---|
# MAGIC | Map end user → SP | caller service | caller service (unchanged) |
# MAGIC | Get SP token | client id + secret → `/oidc/v1/token` | **same** |
# MAGIC | Call agent | `POST /serving-endpoints/<name>/invocations` | `POST https://<app-url>/responses` |
# MAGIC | Identity inside the agent | `ModelServingUserCredentials` (OBO) | `x-forwarded-access-token` injected by the Apps proxy (OBO) |
# MAGIC | Genie / UC row filters | evaluated as the caller SP | **evaluated as the caller SP** |
# MAGIC
# MAGIC **How it works**
# MAGIC 1. **Front door:** every app request needs a Databricks identity. An SP gets one with OAuth M2M
# MAGIC    (client credentials) and must hold `CAN_USE` on the app. Without a token the proxy returns a 302 to login.
# MAGIC 2. **OBO:** the app declares `user_api_scopes: [genie, sql]`. The proxy validates the SP's token, **strips**
# MAGIC    `Authorization`, and injects `x-forwarded-access-token`, a token for the *same SP* limited to those scopes.
# MAGIC 3. The agent's Genie tool uses that forwarded token, so Genie, the SQL warehouse, and the UC row filter all
# MAGIC    see the caller SP. The LLM is called as the app's own SP.
# MAGIC
# MAGIC Two SPs are mapped to regions by a row filter: **EAST** only sees EAST rows, **WEST** only sees WEST rows.

# COMMAND ----------

# MAGIC %pip install databricks-sdk~=0.145 openai requests pandas

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json
import os

# Optional per-workspace widget defaults: notebooks/local_defaults.json is gitignored but synced by the bundle.
LOCAL_DEFAULTS: dict[str, str] = (
    json.load(open("local_defaults.json")) if os.path.exists("local_defaults.json") else {}
)


def widget(name: str, default: str) -> None:
    dbutils.widgets.text(name, LOCAL_DEFAULTS.get(name, default))


widget("app_name", "sp-genie-obo")
widget("east_secret_scope", "retail_consumer_goods")
widget("east_client_id_key", "RETAIL_AI_DATABRICKS_CLIENT_ID")
widget("east_client_secret_key", "RETAIL_AI_DATABRICKS_CLIENT_SECRET")
widget("west_secret_scope", "sp-rls-demo")
widget("west_client_id_key", "west_client_id")
widget("west_client_secret_key", "west_client_secret")
widget("question", "What are total covers and number of reservations by region?")

app_name: str = dbutils.widgets.get("app_name")
question: str = dbutils.widgets.get("question")

def secret(scope: str, key: str) -> str:
    """Read a secret, with an actionable error instead of a raw gRPC trace."""
    try:
        return dbutils.secrets.get(scope, key)
    except Exception as e:
        raise RuntimeError(
            f"Could not read secret '{key}' from scope '{scope}'. Run 00_setup_rls_demo first (it creates the "
            "WEST scope), or set the *_secret_scope / *_key widgets to where your SP credentials live."
        ) from e


# Each SP's credentials come from a secret scope, the way the caller service would hold them.
SPS: dict[str, tuple[str, str]] = {
    region: (
        secret(dbutils.widgets.get(f"{p}_secret_scope"), dbutils.widgets.get(f"{p}_client_id_key")),
        secret(dbutils.widgets.get(f"{p}_secret_scope"), dbutils.widgets.get(f"{p}_client_secret_key")),
    )
    for region, p in (("EAST", "east"), ("WEST", "west"))
}

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
host: str = w.config.host
app_url: str = w.apps.get(app_name).url
print(f"Workspace: {host}\nApp:       {app_url}")

# COMMAND ----------

# MAGIC %md ## 1. Negotiate an OAuth M2M token for each SP
# MAGIC Standard OAuth 2.0 client-credentials grant against the workspace token endpoint. This is the call the
# MAGIC caller service makes today for Model Serving, unchanged. Tokens last about 1 hour, so cache and refresh them.

# COMMAND ----------

import base64
import json
import time

import requests


def mint_m2m_token(client_id: str, client_secret: str) -> str:
    """OAuth 2.0 client credentials → workspace access token for the SP."""
    resp = requests.post(
        f"{host}/oidc/v1/token",
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def jwt_claims(token: str) -> dict:
    """Decode (not verify) the JWT payload, just to show what the token carries."""
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


tokens: dict[str, str] = {region: mint_m2m_token(*creds) for region, creds in SPS.items()}
for region, token in tokens.items():
    claims = jwt_claims(token)
    print(f"{region}: scope={claims.get('scope')!r} expires_in={int(claims['exp'] - time.time())}s")

# COMMAND ----------

# MAGIC %md
# MAGIC The SDK does the same exchange (and refreshes) with `auth_type="oauth-m2m"`. Use this in a long-running caller.

# COMMAND ----------

from databricks.sdk.config import Config

sdk_config = Config(host=host, client_id=SPS["EAST"][0], client_secret=SPS["EAST"][1], auth_type="oauth-m2m")
sdk_auth_header: str = sdk_config.authenticate()["Authorization"]
print("SDK minted header:", sdk_auth_header.split(" ")[0], "<token>")
sp_names: dict[str, str] = {
    region: WorkspaceClient(
        config=Config(host=host, client_id=cid, client_secret=sec, auth_type="oauth-m2m")
    ).current_user.me().display_name
    for region, (cid, sec) in SPS.items()
}
print(sp_names)

# COMMAND ----------

# MAGIC %md ## 2. Front door: no token is rejected, an SP token is accepted

# COMMAND ----------

anon = requests.get(f"{app_url}/whoami", allow_redirects=False, timeout=30)
print("No token →", anon.status_code, anon.headers.get("location", "")[:80])
assert anon.status_code in (302, 401), anon.status_code

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. What the app sees: `/whoami`
# MAGIC A debug route that reports the headers the Apps proxy forwarded and who the forwarded token resolves to.

# COMMAND ----------


def whoami(token: str) -> dict:
    resp = requests.get(f"{app_url}/whoami", headers={"Authorization": f"Bearer {token}"}, timeout=60)
    resp.raise_for_status()
    return resp.json()


app_sp: str | None = None
for region, token in tokens.items():
    info = whoami(token)
    sp_id = SPS[region][0]
    # Compare to the SP's own application id (secret values print as [REDACTED] in Databricks).
    assert info["has_forwarded_access_token"], info
    assert info["token_resolves_to"] == sp_id, "forwarded token is not the caller SP"
    assert info["token_resolves_to"] != info["app_service_principal"], "forwarded token is the app SP"
    app_sp = info["app_service_principal"]
    print(f"{region} ({sp_names[region]}): forwarded token present, resolves to the caller SP ✔")
print(f"(App's own SP, used only for the LLM: {app_sp})")

# COMMAND ----------

# MAGIC %md ## 4. Inference: `POST /responses` as each SP

# COMMAND ----------


def ask(token: str, text: str) -> dict:
    resp = requests.post(
        f"{app_url}/responses",
        headers={"Authorization": f"Bearer {token}"},
        json={"input": [{"role": "user", "content": text}]},
        timeout=300,
    )
    resp.raise_for_status()
    return resp.json()


def final_text(response: dict) -> str:
    return "".join(
        c.get("text", "")
        for item in response["output"] if item["type"] == "message"
        for c in item["content"]
    )


def genie_output(response: dict) -> str:
    return "\n".join(i["output"] for i in response["output"] if i["type"] == "function_call_output")


results: dict[str, dict] = {region: ask(token, question) for region, token in tokens.items()}
for region, response in results.items():
    print(f"=== {region} SP ({sp_names[region]}) ===")
    print("Genie ran as:", response["custom_outputs"]["genie_identity_display_name"])
    print(final_text(response), "\n")

# COMMAND ----------

# MAGIC %md ## 5. Verify: Genie ran as the caller SP, and the row filter applied

# COMMAND ----------

import pandas as pd

for region, response in results.items():
    other = "WEST" if region == "EAST" else "EAST"
    tool_output = genie_output(response)
    assert response["custom_outputs"]["genie_identity"] == SPS[region][0], f"{region}: Genie did not run as the caller SP"
    assert region in tool_output and other not in tool_output, f"{region} SP saw {other} rows:\n{tool_output}"
    assert '"reasoning"' not in final_text(response), "raw model reasoning leaked into the answer"

pd.DataFrame([
    {
        "caller SP": f"{region} ({sp_names[region]})",
        "Genie identity": r["custom_outputs"]["genie_identity_display_name"],
        "regions in Genie result": sorted({x for x in ("EAST", "WEST") if x in genie_output(r)}),
        "answer": final_text(r)[:200],
    }
    for region, r in results.items()
])

# COMMAND ----------

# MAGIC %md ## 6. Streaming with the OpenAI client
# MAGIC The app speaks the OpenAI Responses protocol, so any OpenAI client works.
# MAGIC `OpenAI(api_key=token)` would lock in one token and fail after an hour, so a long-lived client sends a fresh
# MAGIC `Authorization` header on each call via `extra_headers`, using the `sp_auth` helper (see section 7).

# COMMAND ----------

from openai import OpenAI

from sp_auth import SPAuth, sp_auth_headers, sp_config

# One long-lived client; the placeholder api_key is overridden by the per-call header.
client = OpenAI(base_url=app_url, api_key="set-per-call")
stream = client.responses.create(
    model="agent",  # required by the client; the app ignores it
    input=[{"role": "user", "content": question}],
    stream=True,
    extra_headers=sp_auth_headers(host, *SPS["WEST"]),
)
streamed: list[str] = []
for event in stream:
    if event.type == "response.output_item.done":
        print(f"[{event.item.type}]", str(event.item.model_dump())[:160])
        streamed.append(json.dumps(event.item.model_dump()))
assert any('"function_call_output"' in s for s in streamed), "no Genie tool call in stream"
assert not any("EAST" in s for s in streamed if '"function_call_output"' in s), "WEST SP saw EAST rows"
print("\nStreaming as WEST SP verified ✔")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Production token management: cache and refresh, don't re-fetch on every request
# MAGIC
# MAGIC * The M2M token is an OAuth access token (JWT), **valid for 1 hour**. The client-credentials grant issues
# MAGIC   **no refresh token**; refreshing means running the grant again with the client id and secret.
# MAGIC * Don't fetch a token per request: that adds a token-endpoint round trip to every call and puts load on the endpoint.
# MAGIC * `sp_auth.py` (next to this notebook) keeps **one Databricks SDK `Config` per SP**. The SDK caches the token
# MAGIC   (thread-safe), refreshes it **in the background** during the last `min(TTL/2, 20 min)` of its life, and only
# MAGIC   **blocks** a request if the token is within **40 s** of expiring. `SPAuth` also retries once on a 401.
# MAGIC
# MAGIC The cells below prove this with real tokens. They **count real calls** to the token endpoint and
# MAGIC **shift the SDK's clock** forward to simulate the passage of an hour. Both are test-only patches, undone at the end.

# COMMAND ----------

import databricks.sdk.oauth as sdk_oauth
from datetime import datetime, timedelta

_real_retrieve_token, _real_datetime = sdk_oauth.retrieve_token, sdk_oauth.datetime
token_fetches: list[datetime] = []


def _counting_retrieve_token(*args, **kwargs):
    token_fetches.append(datetime.now())
    return _real_retrieve_token(*args, **kwargs)


clock_offset = timedelta(0)


class _ShiftedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime.now(tz) + clock_offset


sdk_oauth.retrieve_token = _counting_retrieve_token
sdk_oauth.datetime = _ShiftedDatetime

# A brand-new SP entry so the counter sees this Config's first fetch (section 6 already cached WEST).
east = (host, *SPS["EAST"])
auth = SPAuth(*east)


def bearer() -> str:
    return sp_auth_headers(*east)["Authorization"]


def call_whoami() -> dict:
    resp = requests.get(f"{app_url}/whoami", auth=auth, timeout=60)
    resp.raise_for_status()
    return resp.json()

# COMMAND ----------

# MAGIC %md ### 7a. Reuse: many requests, one token fetch

# COMMAND ----------

try:
    tok_1 = bearer()
    for _ in range(5):
        assert call_whoami()["token_resolves_to"] == SPS["EAST"][0]
    assert {bearer() for _ in range(20)} == {tok_1}
    expiry = sp_config(*east).oauth_token().expiry
    print(f"5 app calls + 20 header lookups → {len(token_fetches)} token fetch(es); token valid until {expiry:%H:%M:%S}")
    assert len(token_fetches) == 1, token_fetches
    print("Reuse verified ✔")
except Exception:
    sdk_oauth.retrieve_token, sdk_oauth.datetime = _real_retrieve_token, _real_datetime
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. Background refresh: about 45 minutes in, the current token is still served while a new one is fetched

# COMMAND ----------

import time

try:
    clock_offset = timedelta(minutes=45)
    served = bearer()  # stale: returns the current token immediately, refresh starts in the background
    assert served == tok_1, "stale token should still be served without blocking"
    for _ in range(20):
        if len(token_fetches) == 2 and bearer() != tok_1:
            break
        time.sleep(0.5)
    tok_2 = bearer()
    print(f"token fetches: {len(token_fetches)}; token changed: {tok_2 != tok_1}")
    assert len(token_fetches) == 2 and tok_2 != tok_1
    assert call_whoami()["token_resolves_to"] == SPS["EAST"][0]
    print("Background refresh verified ✔ (no request waited for the token endpoint)")
except Exception:
    sdk_oauth.retrieve_token, sdk_oauth.datetime = _real_retrieve_token, _real_datetime
    raise

# COMMAND ----------

# MAGIC %md ### 7c. Blocking refresh: within 40 s of expiry, the next request fetches a new token first

# COMMAND ----------

try:
    expiry_2 = sp_config(*east).oauth_token().expiry
    # Move the shifted clock to 20 s before tok_2 expires.
    clock_offset += (expiry_2 - _ShiftedDatetime.now()) - timedelta(seconds=20)
    tok_3 = bearer()
    print(f"token fetches: {len(token_fetches)}; token changed: {tok_3 != tok_2}")
    assert len(token_fetches) == 3 and tok_3 != tok_2
    assert call_whoami()["token_resolves_to"] == SPS["EAST"][0]
    print("Blocking refresh near expiry verified ✔")
finally:
    sdk_oauth.retrieve_token, sdk_oauth.datetime = _real_retrieve_token, _real_datetime
    print("Test patches removed.")
