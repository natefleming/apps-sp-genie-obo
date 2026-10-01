# SP (OAuth M2M) → Databricks App → Genie, as the caller SP

Shows that an agent on **Databricks Apps** keeps a common Model Serving identity pattern:
a caller service maps each user to a service principal, mints an M2M token for it, and calls the agent.
Genie, the SQL warehouse, and UC row filters then all run **as that SP**.

```
caller SP --(client id/secret → /oidc/v1/token)--> Bearer token
   --> Apps proxy: checks CAN_USE, strips Authorization, injects x-forwarded-access-token (same SP, scopes genie+sql)
   --> MLflow Agent Server /responses --> create_agent (Claude Sonnet 5, runs as the App SP)
   --> ask_genie tool --> Genie Agent API using the forwarded token --> row filter applies to the caller SP
```

## What's here

| Path | What |
|---|---|
| `app/agent.py` | `langchain.agents.create_agent` + one `ask_genie` tool. The tool refuses to run without a forwarded token. |
| `app/app.py` | MLflow `AgentServer` (`/responses`, `/invocations`) plus a `/whoami` debug route. |
| `resources/app.yml` | App with `user_api_scopes: [genie, sql]` and the LLM endpoint resource. |
| `notebooks/00_setup_rls_demo.py` | Creates/reuses SPs, table, row filter, grants, Genie space, and app `CAN_USE`, then checks RLS per SP via SQL. |
| `notebooks/01_sp_m2m_inference.py` | The demo: mint M2M tokens, front-door check, `/whoami`, inference per SP, RLS asserts, OpenAI-client streaming, token-refresh proof. |
| `notebooks/sp_auth.py` | Reusable caller-side helper: one cached, auto-refreshing M2M token per SP (`sp_auth_headers`, `SPAuth` for `requests` with retry-once on 401). |

## Run it (target `fevm`)

1. Deploy and start the app:
   ```bash
   databricks bundle deploy -t fevm
   databricks bundle run sp_genie_obo -t fevm
   ```
2. Run setup (idempotent). It prints the Genie space id:
   ```bash
   databricks bundle run setup_rls_demo -t fevm
   ```
   Set `genie_space_id` to that id, then repeat step 1. Use `--var genie_space_id=<id>`, or put it in the
   gitignored `.databricks/bundle/fevm/variable-overrides.json`:
   ```json
   { "genie_space_id": "<id>", "app_name": "my-sp-genie-obo", "workspace_folder": "my-folder/apps-sp-genie-obo" }
   ```

All names (app, jobs, schema, WEST SP, secret scope, Genie title, workspace folder) are bundle variables in
`databricks.yml` with generic defaults. Change `targets.fevm.workspace.profile` to use your own CLI profile.

If you change any names, mirror them for **interactive** runs (Run All in the workspace uses widget defaults, not
bundle variables). Copy `notebooks/local_defaults.example.json` to `notebooks/local_defaults.json` (gitignored) and
edit it. The bundle syncs it next to the notebooks, and they use it for their widget defaults.
3. Run the demo (or open `notebooks/01_sp_m2m_inference` in the workspace and click **Run All**):
   ```bash
   databricks bundle run sp_m2m_inference_demo -t fevm
   ```

SP credentials: EAST is the existing SP in secret scope `retail_consumer_goods` (`RETAIL_AI_DATABRICKS_CLIENT_ID/_SECRET`).
WEST (`rls-demo-west` by default) is created by setup, with its secret in scope `sp-rls-demo`.

## Verified live on fevm (2026-10-01)

| Check | Result |
|---|---|
| No token → app | `302` to `/oidc/oauth2/v2.0/authorize` |
| SP M2M token → `/whoami` | forwarded token present and resolves to the **caller SP** (not the App SP) |
| EAST SP → "covers by region" | Genie ran as the EAST SP; EAST only: 8 reservations / 34 covers |
| WEST SP → same question | Genie ran as the WEST SP; WEST only: 8 reservations / 32 covers |
| Direct SQL as each SP (setup) | same numbers as above |
| OpenAI client streaming as WEST | Genie tool call streamed, WEST rows only |

## Token lifetime and refresh (caller side)
- The `/oidc/v1/token` result is an OAuth access token (JWT), not a PAT. It is **valid for 1 hour**.
- Client credentials issues **no refresh token**. To refresh, run the grant again with the client id and secret.
- **Cache one token per SP; don't fetch per request.** `sp_auth.py` keeps one Databricks SDK `Config` per SP. The SDK refreshes in the background during the last `min(TTL/2, 20 min)` and blocks only within 40 s of expiry. It's thread-safe.
- With a long-lived OpenAI client, pass `extra_headers=sp_auth_headers(...)` per call; `api_key=token` goes stale after an hour.
- Verified live (notebook section 7, real tokens, shifted SDK clock): 5 app calls + 20 lookups → 1 fetch; at +45 min the current token kept being served while a background refresh fetched a new one; 20 s before expiry, the next call refreshed before sending. The 401 retry was tested locally against a stub server (it's hard to force a real 401 on demand).

## Gotchas hit while building
- Claude Sonnet 5 rejects `temperature`, so don't pass it to `ChatDatabricks`.
- `ChatDatabricks` JSON-serializes list content (reasoning + text) into a string; `app.py:_answer_text` keeps only the text blocks.
- `%pip install pkg>=x` without quotes: the shell treats `>` as a redirect. Quoting fixes the shell, but a quoted spec copied into a serverless notebook's Environment panel keeps its quotes and fails to install. Use `~=` specs (no `>`, no quotes).
- On a serverless job, use `%pip` and declare deps in the job environment; `%uv` wasn't available there.
- An empty env `value` fails app deploy; use a placeholder.
