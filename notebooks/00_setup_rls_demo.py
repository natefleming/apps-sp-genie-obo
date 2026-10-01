# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup: row-level-security demo for SP → Databricks App → Genie
# MAGIC
# MAGIC Creates everything the demo needs, then **proves the row filter works per service principal**
# MAGIC by querying the table as each SP (M2M token → SQL Statement Execution API):
# MAGIC
# MAGIC 1. Resolve the **EAST** SP from an existing secret scope; create the **WEST** SP and store its secret.
# MAGIC 2. Table `reservations` with a UC **row filter** keyed on `current_user()` (an SP's `current_user()` is its application id).
# MAGIC 3. Grants (catalog/schema/table, SQL warehouse, `CAN_USE` on the app) for both SPs.
# MAGIC 4. A Genie space over the table, `CAN_RUN` for both SPs.
# MAGIC 5. Validation: each SP sees only its own region.
# MAGIC
# MAGIC Idempotent — safe to re-run.

# COMMAND ----------

# MAGIC %pip install databricks-sdk~=0.145

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


widget("catalog", "retail_consumer_goods")
widget("schema", "sp_rls_demo")
widget("warehouse_id", "d58e5fb998498840")
widget("east_secret_scope", "retail_consumer_goods")
widget("east_client_id_key", "RETAIL_AI_DATABRICKS_CLIENT_ID")
widget("east_client_secret_key", "RETAIL_AI_DATABRICKS_CLIENT_SECRET")
widget("west_secret_scope", "sp-rls-demo")
widget("west_sp_name", "rls-demo-west")
widget("app_name", "sp-genie-obo")
widget("genie_title", "SP RLS Demo — Reservations")

catalog: str = dbutils.widgets.get("catalog")
schema: str = dbutils.widgets.get("schema")
warehouse_id: str = dbutils.widgets.get("warehouse_id")
east_scope: str = dbutils.widgets.get("east_secret_scope")
east_id_key: str = dbutils.widgets.get("east_client_id_key")
east_secret_key: str = dbutils.widgets.get("east_client_secret_key")
west_scope: str = dbutils.widgets.get("west_secret_scope")
west_sp_name: str = dbutils.widgets.get("west_sp_name")
app_name: str = dbutils.widgets.get("app_name")
genie_title: str = dbutils.widgets.get("genie_title")
table_fqn: str = f"{catalog}.{schema}.reservations"

# COMMAND ----------

# MAGIC %md ## 1. Service principals

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.errors import NotFound

w = WorkspaceClient()
host: str = w.config.host


def sp_client(client_id: str, client_secret: str) -> WorkspaceClient:
    """WorkspaceClient authenticated as an SP via OAuth M2M (client credentials)."""
    return WorkspaceClient(
        config=Config(host=host, client_id=client_id, client_secret=client_secret, auth_type="oauth-m2m")
    )


# EAST: existing SP whose credentials already live in a secret scope.
east_creds: tuple[str, str] = (
    dbutils.secrets.get(east_scope, east_id_key),
    dbutils.secrets.get(east_scope, east_secret_key),
)
east_me = sp_client(*east_creds).current_user.me()
east_app_id: str = east_me.user_name  # an SP's user_name is its application id
print(f"EAST SP: {east_me.display_name} ({east_app_id})")

# WEST: create (or reuse) an SP and keep its OAuth secret in a dedicated scope.
existing = list(w.service_principals.list(filter=f'displayName eq "{west_sp_name}"'))
west_sp = existing[0] if existing else w.service_principals.create(display_name=west_sp_name, active=True)
west_app_id: str = west_sp.application_id
print(f"WEST SP: {west_sp_name} ({west_app_id})")

if west_scope not in {s.name for s in w.secrets.list_scopes()}:
    w.secrets.create_scope(scope=west_scope)
west_keys: set[str] = {s.key for s in w.secrets.list_secrets(scope=west_scope)}
if "west_client_secret" not in west_keys:
    secret = w.service_principal_secrets_proxy.create(service_principal_id=west_sp.id)
    w.secrets.put_secret(scope=west_scope, key="west_client_id", string_value=west_app_id)
    w.secrets.put_secret(scope=west_scope, key="west_client_secret", string_value=secret.secret)
    print("Created WEST OAuth secret and stored it in the scope.")
west_creds: tuple[str, str] = (
    dbutils.secrets.get(west_scope, "west_client_id"),
    dbutils.secrets.get(west_scope, "west_client_secret"),
)


def in_admins(client: WorkspaceClient) -> bool:
    return any(g.display == "admins" for g in (client.current_user.me().groups or []))


# The row filter lets workspace admins see everything; a demo SP must not be an admin.
for label, creds in (("EAST", east_creds), ("WEST", west_creds)):
    assert not in_admins(sp_client(*creds)), f"{label} SP is a workspace admin; pick a non-admin SP."

# COMMAND ----------

# MAGIC %md ## 2. Table + row filter

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"""
CREATE OR REPLACE TABLE {table_fqn} (
  reservation_id INT, restaurant STRING, city STRING, region STRING,
  reservation_date DATE, covers INT
) COMMENT 'Synthetic restaurant reservations for the SP RLS demo.'
""")
spark.sql(f"""
INSERT INTO {table_fqn} VALUES
  (1,'Harbor Oyster Bar','Boston','EAST',DATE'2026-09-01',4),
  (2,'Harbor Oyster Bar','Boston','EAST',DATE'2026-09-02',2),
  (3,'Liberty Steakhouse','New York','EAST',DATE'2026-09-01',6),
  (4,'Liberty Steakhouse','New York','EAST',DATE'2026-09-03',3),
  (5,'Peachtree Kitchen','Atlanta','EAST',DATE'2026-09-02',5),
  (6,'Peachtree Kitchen','Atlanta','EAST',DATE'2026-09-04',2),
  (7,'Capitol Bistro','Washington','EAST',DATE'2026-09-05',4),
  (8,'Capitol Bistro','Washington','EAST',DATE'2026-09-06',8),
  (9,'Golden Gate Grill','San Francisco','WEST',DATE'2026-09-01',2),
  (10,'Golden Gate Grill','San Francisco','WEST',DATE'2026-09-02',3),
  (11,'Sunset Taqueria','Los Angeles','WEST',DATE'2026-09-01',7),
  (12,'Sunset Taqueria','Los Angeles','WEST',DATE'2026-09-03',4),
  (13,'Emerald Sushi','Seattle','WEST',DATE'2026-09-02',2),
  (14,'Emerald Sushi','Seattle','WEST',DATE'2026-09-05',6),
  (15,'Desert Rose Cantina','Phoenix','WEST',DATE'2026-09-04',5),
  (16,'Desert Rose Cantina','Phoenix','WEST',DATE'2026-09-06',3)
""")

# An SP's current_user() is its application id; map each SP to the one region it may see.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_member('admins') OR region = CASE current_user()
  WHEN '{east_app_id}' THEN 'EAST'
  WHEN '{west_app_id}' THEN 'WEST'
END
""")
spark.sql(f"ALTER TABLE {table_fqn} SET ROW FILTER {catalog}.{schema}.region_filter ON (region)")

# COMMAND ----------

# MAGIC %md ## 3. Grants

# COMMAND ----------

from databricks.sdk.service import sql as sql_svc
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

for app_id in (east_app_id, west_app_id):
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{app_id}`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{app_id}`")
    spark.sql(f"GRANT SELECT ON TABLE {table_fqn} TO `{app_id}`")

w.warehouses.update_permissions(
    warehouse_id=warehouse_id,
    access_control_list=[
        sql_svc.WarehouseAccessControlRequest(
            service_principal_name=app_id, permission_level=sql_svc.WarehousePermissionLevel.CAN_USE
        )
        for app_id in (east_app_id, west_app_id)
    ],
)

# Front door: each SP may call the app (CAN_USE). Done here, not in the bundle, because the SPs are created above.
from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel

w.apps.update_permissions(
    app_name=app_name,
    access_control_list=[
        AppAccessControlRequest(service_principal_name=app_id, permission_level=AppPermissionLevel.CAN_USE)
        for app_id in (east_app_id, west_app_id)
    ],
)

# COMMAND ----------

# MAGIC %md ## 4. Genie space

# COMMAND ----------

import json

serialized_space: str = json.dumps({
    "version": 2,
    "config": {"sample_questions": [
        {"id": "a" * 32, "question": ["What are total covers by region?"]},
    ]},
    "data_sources": {"tables": [{"identifier": table_fqn}]},
    "instructions": {"text_instructions": [{"id": "c" * 32, "content": [
        "Each row is one restaurant reservation; covers is the party size. "
        "Answer only from the rows returned by the query."
    ]}]},
})

spaces = w.genie.list_spaces().spaces or []
space_id: str | None = next((s.space_id for s in spaces if s.title == genie_title), None)
if space_id is None:
    space_id = w.genie.create_space(
        warehouse_id=warehouse_id, serialized_space=serialized_space, title=genie_title,
        description="Service principal row-level-security demo",
    ).space_id
else:
    w.genie.update_space(space_id=space_id, serialized_space=serialized_space, warehouse_id=warehouse_id)

w.permissions.update(
    "genie", space_id,
    access_control_list=[
        AccessControlRequest(service_principal_name=app_id, permission_level=PermissionLevel.CAN_RUN)
        for app_id in (east_app_id, west_app_id)
    ],
)
print(f"Genie space: {space_id}")

# COMMAND ----------

# MAGIC %md ## 5. Validate the row filter *as each SP*

# COMMAND ----------

QUERY: str = f"SELECT region, count(*) AS reservations, sum(covers) AS covers FROM {table_fqn} GROUP BY region ORDER BY region"


def query_as(creds: tuple[str, str]) -> list[list[str]]:
    resp = sp_client(*creds).statement_execution.execute_statement(
        statement=QUERY, warehouse_id=warehouse_id, wait_timeout="50s"
    )
    while resp.status.state in (sql_svc.StatementState.PENDING, sql_svc.StatementState.RUNNING):
        time.sleep(2)
        resp = sp_client(*creds).statement_execution.get_statement(resp.statement_id)
    assert resp.status.state == sql_svc.StatementState.SUCCEEDED, resp.status
    return resp.result.data_array or []


east_rows = query_as(east_creds)
west_rows = query_as(west_creds)
print("EAST SP sees:", east_rows)
print("WEST SP sees:", west_rows)
assert [r[0] for r in east_rows] == ["EAST"], east_rows
assert [r[0] for r in west_rows] == ["WEST"], west_rows
print("Row filter verified per SP.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "east_sp_application_id": east_app_id,
    "west_sp_application_id": west_app_id,
    "genie_space_id": space_id,
    "expected": {"EAST": east_rows, "WEST": west_rows},
}))
