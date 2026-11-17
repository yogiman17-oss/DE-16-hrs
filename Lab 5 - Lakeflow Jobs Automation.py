# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Lab 5: Lakeflow Jobs Automation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, we explore **Lakeflow Jobs** — the production orchestration engine of Databricks. Lakeflow Jobs let you schedule, chain, parameterize, and monitor notebook tasks as automated workflows. You will create jobs programmatically using the **Databricks Python SDK**, define multi-task DAGs with dependencies, pass parameters between tasks, configure scheduling, and monitor run results — all from within this notebook.
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC
# MAGIC - Create a Lakeflow Job programmatically using the Databricks SDK
# MAGIC - Define single-task and multi-task workflows
# MAGIC - Configure task dependencies to build DAGs (Directed Acyclic Graphs)
# MAGIC - Pass parameters to notebook tasks using `dbutils.widgets`
# MAGIC - Set up cron-based schedules and file-arrival triggers
# MAGIC - Monitor job runs, retrieve logs, and inspect task-level results
# MAGIC - Configure retry policies, timeouts, and email notifications
# MAGIC - Compare serverless vs job-cluster compute for tasks
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC | Prerequisite | Why Needed |
# MAGIC |---|---|
# MAGIC | **Lab 1 (Data Ingestion)** | Bronze tables (`bronze_customers`, `bronze_orders`, `bronze_products`) must exist |
# MAGIC | **Lab 4 (Merge Operations)** | MERGE patterns referenced in the ETL task |
# MAGIC | **Unity Catalog Access** | `lakemeter_catalog.labuser11` catalog and schema |
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---|---|
# MAGIC | **Job** | A named automation unit containing one or more tasks |
# MAGIC | **Task** | A single executable unit (notebook, SQL, Python script, pipeline, etc.) |
# MAGIC | **DAG** | Directed Acyclic Graph — tasks linked by `depends_on` relationships |
# MAGIC | **Run** | A single execution instance of a job |
# MAGIC | **Trigger** | What starts a job: manual, cron schedule, file arrival, or continuous |
# MAGIC | **Job Cluster** | Ephemeral compute created for the job and terminated after — cost-optimized |
# MAGIC | **Serverless Compute** | Fully managed, zero-configuration compute — recommended for most tasks |
# MAGIC
# MAGIC ### Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │                    LAKEFLOW JOB                          │
# MAGIC │        "Lab5_ETL_Pipeline" (cron: daily 8 AM)           │
# MAGIC ├─────────────────────────────────────────────────────────┤
# MAGIC │                                                         │
# MAGIC │   ┌──────────────┐                                      │
# MAGIC │   │  Task 1       │                                      │
# MAGIC │   │  Ingest Data  │──────┐                               │
# MAGIC │   └──────────────┘      │                               │
# MAGIC │                          ▼                               │
# MAGIC │                   ┌──────────────┐                       │
# MAGIC │                   │  Task 2       │                       │
# MAGIC │                   │  Transform    │──────┐                │
# MAGIC │                   └──────────────┘      │                │
# MAGIC │                                          ▼                │
# MAGIC │                                   ┌──────────────┐       │
# MAGIC │                                   │  Task 3       │       │
# MAGIC │                                   │  Aggregate    │       │
# MAGIC │                                   └──────────────┘       │
# MAGIC │                                          │                │
# MAGIC │                                          ▼                │
# MAGIC │                                   ┌──────────────┐       │
# MAGIC │                                   │  Task 4       │       │
# MAGIC │                                   │  Validate     │       │
# MAGIC │                                   └──────────────┘       │
# MAGIC │                                                         │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section A Intro
# MAGIC %md
# MAGIC ## A. Environment Setup
# MAGIC
# MAGIC Set the default **catalog** and **schema**, and install the Databricks Python SDK which provides a clean, typed interface for managing all Databricks resources programmatically.

# COMMAND ----------

# DBTITLE 1,Set Catalog and Schema
# MAGIC %sql
# MAGIC USE CATALOG lakemeter_catalog;
# MAGIC USE SCHEMA labuser11;

# COMMAND ----------

# DBTITLE 1,Verify Context
# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# DBTITLE 1,Install and Initialize SDK
# ============================================================
# Install the Databricks SDK and initialize the client
# ============================================================
# The SDK provides typed Python classes for every Databricks
# resource: Jobs, Clusters, Pipelines, Unity Catalog, etc.
# When running inside a Databricks notebook, authentication
# is automatic (uses the notebook's execution context).
# ============================================================

%pip install --quiet databricks-sdk --upgrade
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Initialize Workspace Client
# ============================================================
# Initialize the WorkspaceClient
# ============================================================
# WorkspaceClient auto-detects credentials when running in
# a Databricks notebook (no tokens or URLs needed).
# ============================================================

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import *
from databricks.sdk.service.compute import *
from databricks.sdk.service.jobs import JobEnvironment
from databricks.sdk.service.compute import Environment
import json
from datetime import datetime

w = WorkspaceClient()

# Verify connection
current_user = w.current_user.me()
print(f"Connected as: {current_user.user_name}")
print(f"Workspace:    {w.config.host}")

# COMMAND ----------

# DBTITLE 1,Section B Intro
# MAGIC %md
# MAGIC ## B. Understanding Lakeflow Jobs
# MAGIC
# MAGIC A **Lakeflow Job** is the production scheduling and orchestration unit in Databricks. It wraps one or more **tasks** into a repeatable, monitorable workflow.
# MAGIC
# MAGIC ### Job Components
# MAGIC
# MAGIC | Component | Description | Example |
# MAGIC |---|---|---|
# MAGIC | **Job Name** | Human-readable identifier | `Lab5_ETL_Pipeline` |
# MAGIC | **Task** | A single executable unit | Notebook, SQL query, Python script, Pipeline |
# MAGIC | **Dependency** | Task ordering (`depends_on`) | Task 2 waits for Task 1 |
# MAGIC | **Trigger** | When the job runs | Cron, file arrival, continuous, manual |
# MAGIC | **Compute** | Where the task executes | Serverless, Job Cluster, All-Purpose |
# MAGIC | **Parameters** | Runtime inputs to tasks | `{"run_date": "2024-03-01"}` |
# MAGIC | **Notifications** | Alerts on success/failure | Email, webhook, PagerDuty |
# MAGIC | **Retry Policy** | Auto-retry on failure | 2 retries, 60s interval |
# MAGIC
# MAGIC ### Task Types
# MAGIC
# MAGIC | Task Type | Use Case | Configuration |
# MAGIC |---|---|---|
# MAGIC | **Notebook** | Run a Databricks notebook | Path + parameters |
# MAGIC | **SQL** | Execute a SQL query or file | Query text or file path |
# MAGIC | **Python Script** | Run a `.py` file from workspace | File path + args |
# MAGIC | **Pipeline** | Trigger a Lakeflow SDP pipeline | Pipeline ID |
# MAGIC | **dbt** | Run a dbt project | dbt commands |
# MAGIC | **Run Job** | Trigger another job as a task | Job ID (for reuse) |
# MAGIC
# MAGIC ### Compute Options for Tasks
# MAGIC
# MAGIC | Option | Startup Time | Cost | Best For |
# MAGIC |---|---|---|
# MAGIC | **Serverless** | ~10 seconds | Pay-per-use | Most tasks (recommended) |
# MAGIC | **Job Cluster** | ~2–4 minutes | Spot instances possible | Large Spark jobs |
# MAGIC | **All-Purpose** | Already running | Expensive (always-on) | Development only |

# COMMAND ----------

# DBTITLE 1,List Existing Jobs
# ============================================================
# B1. List existing jobs in the workspace
# ============================================================
# The SDK's list_jobs() returns a paginated iterator.
# We filter to jobs owned by the current user.
# ============================================================

my_jobs = [j for j in w.jobs.list(expand_tasks=False, limit=20)]

print(f"Total jobs visible: {len(my_jobs)}")
print(f"{'Job ID':<15} {'Job Name':<45} {'Creator'}")
print("-" * 80)
for j in my_jobs[:10]:
    print(f"{j.job_id:<15} {j.settings.name[:44]:<45} {j.creator_user_name}")

# COMMAND ----------

# DBTITLE 1,Section C Intro
# MAGIC %md
# MAGIC ## C. Creating a Single-Task Job
# MAGIC
# MAGIC ### What Happens Here
# MAGIC We create a simple job with **one notebook task** that runs a SQL validation query. This is the simplest job pattern — ideal for scheduled reports or single-step ETL.
# MAGIC
# MAGIC ### Internal Mechanism
# MAGIC 1. `w.jobs.create()` sends a POST to the Jobs API with the job specification
# MAGIC 2. Databricks registers the job with a unique `job_id`
# MAGIC 3. The job sits idle until triggered (manually, by schedule, or by event)
# MAGIC 4. When triggered, Databricks provisions the specified compute, runs the task, and reports results
# MAGIC
# MAGIC ### C1. Creating a Validation Notebook
# MAGIC
# MAGIC First, we create a small notebook that the job will execute. This notebook validates that our Bronze tables from Lab 1 exist and have data.

# COMMAND ----------

# DBTITLE 1,Create Task Notebook
# ============================================================
# C1. Create a validation notebook for the job to execute
# ============================================================
# This notebook checks Bronze table health: existence, row
# counts, and freshness. It uses dbutils.widgets for
# parameterization (the job will pass values at runtime).
# ============================================================

import base64

notebook_content = """
# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Table Validation
# MAGIC Automated validation task — called by Lakeflow Job

# COMMAND ----------
# Widget parameters (set by the job at runtime)
dbutils.widgets.text("catalog", "lakemeter_catalog", "Catalog")
dbutils.widgets.text("schema", "labuser11", "Schema")
dbutils.widgets.text("run_date", "", "Run Date")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
from datetime import datetime
run_date = dbutils.widgets.get("run_date") or str(datetime.now().date())

print(f"Validating: {catalog}.{schema} | Run Date: {run_date}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

tables = ["bronze_customers", "bronze_products", "bronze_orders"]
results = []

for t in tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {t}").collect()[0]["cnt"]
        results.append({"table": t, "row_count": count, "status": "PASS" if count > 0 else "FAIL"})
    except Exception as e:
        results.append({"table": t, "row_count": 0, "status": f"ERROR: {str(e)[:80]}"})

for r in results:
    print(f"  {r['table']:<25} rows={r['row_count']:<8} status={r['status']}")

failed = [r for r in results if r["status"] != "PASS"]
if failed:
    raise Exception(f"Validation FAILED for: {[r['table'] for r in failed]}")

print(f"\\nAll {len(tables)} Bronze tables validated successfully!")

# COMMAND ----------
# Return a summary for downstream tasks (task values)
dbutils.notebook.exit(f"PASS|{len(tables)} tables|{sum(r['row_count'] for r in results)} total rows")
"""

# Write the notebook to workspace
notebook_path = f"/Users/{current_user.user_name}/1A. Data Engg_v2/2. Jobs/_task_validate_bronze"

try:
    w.workspace.delete(notebook_path)
except:
    pass

from databricks.sdk.service.workspace import ImportFormat, Language

w.workspace.import_(
    path=notebook_path,
    content=base64.b64encode(notebook_content.encode()).decode(),
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True
)

print(f"Task notebook created: {notebook_path}")

# COMMAND ----------

# DBTITLE 1,C2 Create Single-Task Job
# MAGIC %md
# MAGIC ### C2. Creating the Job via SDK
# MAGIC
# MAGIC The `w.jobs.create()` call defines:
# MAGIC - **Job name** — must be unique per workspace (best practice)
# MAGIC - **Task** — a notebook task pointing to our validation notebook
# MAGIC - **Parameters** — key-value pairs passed to `dbutils.widgets`
# MAGIC - **Compute** — serverless (zero configuration, auto-scales)

# COMMAND ----------

# DBTITLE 1,Create Single-Task Job
# ============================================================
# C2. Create a single-task Lakeflow Job
# ============================================================
# Serverless compute is the recommended default:
# - No cluster configuration needed
# - ~10 second startup
# - Pay only for execution time
# - Auto-scales based on workload
# ============================================================

JOB_NAME = f"Lab5_Bronze_Validation_{current_user.user_name.split('@')[0]}"

# Clean up any previous run of this lab
for j in w.jobs.list(name=JOB_NAME):
    w.jobs.delete(j.job_id)
    print(f"Deleted previous job: {j.job_id}")

# Create the job
job = w.jobs.create(
    name=JOB_NAME,
    tasks=[
        Task(
            task_key="validate_bronze",
            description="Validate Bronze tables exist and have data",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters={
                    "catalog": "lakemeter_catalog",
                    "schema":  "labuser11",
                    "run_date": "{{job.start_time.iso_date}}"
                }
            ),
            # Serverless compute — no cluster config needed
            environment_key="Default"
        )
    ],
    # Define the serverless environment
    environments=[
        JobEnvironment(
            environment_key="Default",
            spec=Environment(client="1")
        )
    ],
    # Job-level settings
    max_concurrent_runs=1,
    timeout_seconds=600,       # 10-minute timeout
    tags={"lab": "lab5", "purpose": "validation"}
)

print(f"Job created successfully!")
print(f"  Job ID:   {job.job_id}")
print(f"  Job Name: {JOB_NAME}")
print(f"  Task:     validate_bronze")
print(f"  Compute:  Serverless")
print(f"  URL:      {w.config.host}/jobs/{job.job_id}")

# COMMAND ----------

# DBTITLE 1,Observation C
# MAGIC %md
# MAGIC **Observation:** The job was created with a unique ID. Key points:
# MAGIC - `environment_key="Default"` tells Databricks to use **serverless compute** (no cluster definition needed)
# MAGIC - `base_parameters` are passed as widget values to the notebook at runtime
# MAGIC - `{{job.start_time.iso_date}}` is a **dynamic value reference** — Databricks replaces it with the actual run date at execution time
# MAGIC - `max_concurrent_runs=1` prevents overlapping runs of the same job

# COMMAND ----------

# DBTITLE 1,Section D Intro
# MAGIC %md
# MAGIC ## D. Running a Job and Monitoring Results
# MAGIC
# MAGIC ### What Happens Here
# MAGIC We trigger the job manually using `w.jobs.run_now()`, then poll for completion and inspect the results. In production, jobs run on schedule — but manual triggers are essential for testing.
# MAGIC
# MAGIC ### Run Lifecycle
# MAGIC
# MAGIC ```
# MAGIC run_now()  →  QUEUED  →  PENDING  →  RUNNING  →  TERMINATED
# MAGIC                           (compute       (task         (success or
# MAGIC                            starting)      executing)    failure)
# MAGIC ```
# MAGIC
# MAGIC | State | Meaning |
# MAGIC |---|---|
# MAGIC | `QUEUED` | Run accepted, waiting for a slot |
# MAGIC | `PENDING` | Compute is starting up |
# MAGIC | `RUNNING` | Task code is executing |
# MAGIC | `TERMINATED` | Run finished (check `result_state` for PASS/FAIL) |
# MAGIC | `SKIPPED` | Task skipped (dependency not met) |
# MAGIC | `INTERNAL_ERROR` | Platform error (retry automatically) |

# COMMAND ----------

# DBTITLE 1,Run Job and Wait
# ============================================================
# D1. Trigger the job and wait for completion
# ============================================================
# run_now() starts the job immediately and returns a Run object.
# .result() blocks until the run completes (with timeout).
# ============================================================

print(f"Triggering job: {JOB_NAME} (ID: {job.job_id})")
print(f"Start time: {datetime.now().strftime('%H:%M:%S')}")
print("Waiting for completion...\n")

# Run and wait (blocks until done)
run = w.jobs.run_now(job.job_id).result()

print(f"Run completed!")
print(f"  Run ID:       {run.run_id}")
print(f"  State:        {run.state.life_cycle_state}")
print(f"  Result:       {run.state.result_state}")
print(f"  Duration:     {(run.end_time - run.start_time) / 1000:.1f} seconds")
print(f"  Run URL:      {w.config.host}/jobs/{job.job_id}/runs/{run.run_id}")

# COMMAND ----------

# DBTITLE 1,Inspect Task Results
# ============================================================
# D2. Inspect task-level results
# ============================================================
# Each task in a run has its own result, state, and output.
# For notebook tasks, the output includes the value passed
# to dbutils.notebook.exit().
# ============================================================

for task_run in run.tasks:
    print(f"Task: {task_run.task_key}")
    print(f"  State:      {task_run.state.life_cycle_state}")
    print(f"  Result:     {task_run.state.result_state}")
    
    if task_run.start_time and task_run.end_time:
        duration = (task_run.end_time - task_run.start_time) / 1000
        print(f"  Duration:   {duration:.1f}s")
    
    # Get the notebook output (from dbutils.notebook.exit())
    try:
        output = w.jobs.get_run_output(task_run.run_id)
        if output.notebook_output:
            print(f"  Output:     {output.notebook_output.result}")
    except Exception as e:
        print(f"  Output:     (not available: {e})")
    print()

# COMMAND ----------

# DBTITLE 1,Observation D
# MAGIC %md
# MAGIC **Observation:** The run completed with a `TERMINATED` state and `SUCCESS` result. Key takeaways:
# MAGIC - `run_now().result()` is a **blocking call** — it waits until the entire job finishes (useful for testing)
# MAGIC - In production code, use `run_now()` without `.result()` for async fire-and-forget
# MAGIC - `dbutils.notebook.exit()` returns a string value that downstream tasks or monitoring can inspect
# MAGIC - The task duration shows serverless startup + execution time combined

# COMMAND ----------

# DBTITLE 1,Section E Intro
# MAGIC %md
# MAGIC ## E. Multi-Task DAG (Task Dependencies)
# MAGIC
# MAGIC ### What Happens Here
# MAGIC We create a job with **four tasks** chained in a DAG. This mirrors a real ETL pipeline:
# MAGIC 1. **Ingest** — Load raw data into Bronze
# MAGIC 2. **Transform** — Clean Bronze into Silver (depends on Ingest)
# MAGIC 3. **Aggregate** — Build Gold tables from Silver (depends on Transform)
# MAGIC 4. **Validate** — Quality check on all layers (depends on Aggregate)
# MAGIC
# MAGIC ### DAG Patterns
# MAGIC
# MAGIC | Pattern | Description | Example |
# MAGIC |---|---|---|
# MAGIC | **Linear Chain** | A → B → C | Sequential ETL stages |
# MAGIC | **Fan-Out** | A → [B, C, D] | Parallel independent tasks after a common setup |
# MAGIC | **Fan-In** | [B, C, D] → E | One task waits for multiple predecessors |
# MAGIC | **Diamond** | A → [B, C] → D | Combination of fan-out and fan-in |
# MAGIC
# MAGIC ### E1. Creating Task Notebooks
# MAGIC
# MAGIC We create lightweight notebooks for each ETL stage. Each notebook:
# MAGIC - Accepts `catalog` and `schema` parameters via widgets
# MAGIC - Performs its ETL logic
# MAGIC - Returns a status via `dbutils.notebook.exit()`

# COMMAND ----------

# DBTITLE 1,Create ETL Task Notebooks
# ============================================================
# E1. Create task notebooks for the multi-task DAG
# ============================================================
# Each notebook represents one stage of the ETL pipeline.
# In production, these would be your actual ingestion,
# transformation, and aggregation notebooks.
# ============================================================

base_path = f"/Users/{current_user.user_name}/1A. Data Engg_v2/2. Jobs"
CAT = "lakemeter_catalog"
SCH = "labuser11"

# --- Task 1: Ingest ---
ingest_lines = [
    '# Databricks notebook source',
    '# COMMAND ----------',
    '# MAGIC %md',
    '# MAGIC # Task: Data Ingestion (Bronze)',
    '# COMMAND ----------',
    f'dbutils.widgets.text("catalog", "{CAT}", "Catalog")',
    f'dbutils.widgets.text("schema", "{SCH}", "Schema")',
    'cat = dbutils.widgets.get("catalog")',
    'sch = dbutils.widgets.get("schema")',
    'spark.sql(f"USE CATALOG {cat}")',
    'spark.sql(f"USE SCHEMA {sch}")',
    '# COMMAND ----------',
    'tables = ["bronze_customers", "bronze_products", "bronze_orders"]',
    'total_rows = 0',
    'for t in tables:',
    '    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {t}").collect()[0]["c"]',
    '    total_rows += cnt',
    '    print(f"  {t}: {cnt} rows")',
    'print(f"Ingestion check complete: {total_rows} total rows")',
    'dbutils.notebook.exit(f"INGESTED|{total_rows} rows")',
]

# --- Task 2: Transform ---
transform_lines = [
    '# Databricks notebook source',
    '# COMMAND ----------',
    '# MAGIC %md',
    '# MAGIC # Task: Data Transformation (Silver)',
    '# COMMAND ----------',
    f'dbutils.widgets.text("catalog", "{CAT}", "Catalog")',
    f'dbutils.widgets.text("schema", "{SCH}", "Schema")',
    'cat = dbutils.widgets.get("catalog")',
    'sch = dbutils.widgets.get("schema")',
    'spark.sql(f"USE CATALOG {cat}")',
    'spark.sql(f"USE SCHEMA {sch}")',
    '# COMMAND ----------',
    'pairs = [("bronze_customers", "silver_customers", "customer_id"),',
    '         ("bronze_products", "silver_products", "product_id"),',
    '         ("bronze_orders", "silver_orders", "order_id")]',
    'for src, tgt, key in pairs:',
    '    sql = f"CREATE OR REPLACE TABLE {tgt} AS SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY {key}) AS rn FROM {src}) WHERE rn = 1"',
    '    spark.sql(sql)',
    '    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {tgt}").collect()[0]["c"]',
    '    print(f"  {tgt}: {cnt} rows (deduplicated)")',
    'print("Transformation complete!")',
    'dbutils.notebook.exit("TRANSFORMED|3 Silver tables")',
]

# --- Task 3: Aggregate ---
aggregate_lines = [
    '# Databricks notebook source',
    '# COMMAND ----------',
    '# MAGIC %md',
    '# MAGIC # Task: Data Aggregation (Gold)',
    '# COMMAND ----------',
    f'dbutils.widgets.text("catalog", "{CAT}", "Catalog")',
    f'dbutils.widgets.text("schema", "{SCH}", "Schema")',
    'cat = dbutils.widgets.get("catalog")',
    'sch = dbutils.widgets.get("schema")',
    'spark.sql(f"USE CATALOG {cat}")',
    'spark.sql(f"USE SCHEMA {sch}")',
    '# COMMAND ----------',
    'sql = "CREATE OR REPLACE TABLE gold_order_summary AS SELECT c.customer_id, c.name AS customer_name, COUNT(o.order_id) AS total_orders, ROUND(SUM(o.total_amount), 2) AS total_revenue, MAX(o.order_date) AS last_order_date FROM silver_orders o JOIN silver_customers c ON o.customer_id = c.customer_id GROUP BY c.customer_id, c.name"',
    'spark.sql(sql)',
    'cnt = spark.sql("SELECT COUNT(*) AS c FROM gold_order_summary").collect()[0]["c"]',
    'print(f"  gold_order_summary: {cnt} rows")',
    'print("Aggregation complete!")',
    'dbutils.notebook.exit(f"AGGREGATED|{cnt} customer summaries")',
]

task_notebooks = {
    "_task_ingest": "\n".join(ingest_lines),
    "_task_transform": "\n".join(transform_lines),
    "_task_aggregate": "\n".join(aggregate_lines),
}

# Write all task notebooks as .py workspace files
import io

for name, content in task_notebooks.items():
    path = f"{base_path}/{name}.py"
    try:
        w.workspace.delete(path)
    except:
        pass
    
    w.workspace.upload(
        path=path,
        content=io.BytesIO(content.encode('utf-8')),
        overwrite=True
    )
    print(f"Created: {path}")

print(f"\nAll {len(task_notebooks)} task notebooks created!")

# COMMAND ----------

# DBTITLE 1,E2 Create Multi-Task Job Intro
# MAGIC %md
# MAGIC ### E2. Creating the Multi-Task DAG Job
# MAGIC
# MAGIC Now we wire the notebooks into a **4-task DAG** with dependencies. The `depends_on` parameter creates the execution order:
# MAGIC
# MAGIC ```
# MAGIC ingest → transform → aggregate → validate
# MAGIC ```
# MAGIC
# MAGIC If any task fails, downstream tasks are **automatically skipped** (no wasted compute).

# COMMAND ----------

# DBTITLE 1,Create Multi-Task Job
# ============================================================
# E2. Create the multi-task DAG job
# ============================================================
# Each task references a notebook and declares dependencies.
# The SDK validates the DAG is acyclic (no circular deps).
#
# --- Available Task Types in the Databricks Jobs SDK ---
#
# 1. NotebookTask        – Runs a Databricks notebook (.py, .sql, .scala, .r)
#                          Used below. Set notebook_path + base_parameters.
#
# 2. SparkPythonTask     – Runs a Python file (.py) as a Spark job.
#                          Set python_file (workspace/DBFS/cloud path) + parameters.
#
# 3. SparkJarTask        – Runs a JAR on a JVM-based Spark cluster.
#                          Set main_class_name + parameters. Attach JARs via libraries.
#
# 4. SparkSubmitTask     – Mirrors `spark-submit` CLI; arbitrary Spark args.
#                          Set parameters list (the full spark-submit arg vector).
#
# 5. PipelineTask        – Triggers a Delta Live Tables / Lakeflow pipeline.
#                          Set pipeline_id; optionally full_refresh=True.
#
# 6. PythonWheelTask     – Runs an entry point from an installed Python wheel.
#                          Set package_name + entry_point + named_parameters.
#
# 7. SqlTask             – Runs a DBSQL query, dashboard, or alert.
#                          Set query / dashboard / alert + warehouse_id.
#
# 8. DbtTask             – Runs dbt commands (run, test, seed, etc.).
#                          Set commands list + project_directory + warehouse_id.
#
# 9. RunJobTask          – Triggers another existing job (job-in-job).
#                          Set job_id of the target job + optional job_parameters.
#
# 10. ConditionTask      – Evaluates a condition; controls branching in the DAG.
#                          Set op (e.g. EQUAL_TO) + left/right operands.
#
# 11. ForEachTask        – Loops over an array, running a nested task per element.
#                          Set inputs (JSON array) + task (the inner Task definition).
#
# This lab uses NotebookTask for simplicity, but you can mix
# any of the above within the same DAG job.
# ============================================================

DAG_JOB_NAME = f"Lab5_ETL_DAG_{current_user.user_name.split('@')[0]}"

# Clean up previous
for j in w.jobs.list(name=DAG_JOB_NAME):
    w.jobs.delete(j.job_id)

common_params = {"catalog": "lakemeter_catalog", "schema": "labuser11"}

dag_job = w.jobs.create(
    name=DAG_JOB_NAME,
    tasks=[
        # Task 1: Ingest (no dependencies — runs first)
        Task(
            task_key="ingest",
            description="Ingest raw data into Bronze tables",
            notebook_task=python_task()
        
            notebook_task=NotebookTask(
                notebook_path=f"{base_path}/_task_ingest.py",
                base_parameters=common_params
            ),
            environment_key="Default"
        ),
        # Task 2: Transform (depends on ingest)
        Task(
            task_key="transform",
            description="Clean Bronze into Silver tables",
            depends_on=[TaskDependency(task_key="ingest")],
            notebook_task=NotebookTask(
                notebook_path=f"{base_path}/_task_transform.py",
                base_parameters=common_params
            ),
            environment_key="Default"
        ),
        # Task 3: Aggregate (depends on transform)
        Task(
            task_key="aggregate",
            description="Build Gold summary tables from Silver",
            depends_on=[TaskDependency(task_key="transform")],
            notebook_task=NotebookTask(
                notebook_path=f"{base_path}/_task_aggregate.py",
                base_parameters=common_params
            ),
            environment_key="Default"
        ),
        # Task 4: Validate (depends on aggregate)
        Task(
            task_key="validate",
            description="Validate all pipeline layers",
            depends_on=[TaskDependency(task_key="aggregate")],
            notebook_task=NotebookTask(
                notebook_path=notebook_path,   # Reuse from Section C
                base_parameters=common_params
            ),
            environment_key="Default"
        )
    ],
    # Define the serverless environment
    environments=[
        JobEnvironment(
            environment_key="Default",
            spec=Environment(client="1")
        )
    ],
    max_concurrent_runs=1,
    timeout_seconds=1800,  # 30-minute overall timeout
    tags={"lab": "lab5", "pattern": "dag"}
)

print(f"Multi-task DAG job created!")
print(f"  Job ID:   {dag_job.job_id}")
print(f"  Job Name: {DAG_JOB_NAME}")
print(f"  Tasks:    ingest → transform → aggregate → validate")
print(f"  URL:      {w.config.host}/jobs/{dag_job.job_id}")

# COMMAND ----------

# DBTITLE 1,View Run Logs for Existing Jobs
# ============================================================
# View run logs for all existing jobs
# ============================================================
from datetime import datetime

my_jobs = [j for j in w.jobs.list() if j.creator_user_name == current_user.user_name]
print(f"Found {len(my_jobs)} jobs owned by {current_user.user_name}\n")

rows = []
for j in my_jobs:
    runs = list(w.jobs.list_runs(job_id=j.job_id, limit=3))  # last 3 runs per job
    if not runs:
        rows.append({
            "job_id": j.job_id,
            "job_name": j.settings.name if j.settings else str(j.job_id),
            "run_id": None,
            "state": "NO RUNS",
            "result": "-",
            "start_time": None,
            "duration_sec": None,
            "trigger": "-"
        })
    else:
        for r in runs:
            start = datetime.fromtimestamp(r.start_time / 1000).strftime("%Y-%m-%d %H:%M:%S") if r.start_time else None
            dur = round((r.end_time - r.start_time) / 1000, 1) if (r.end_time and r.start_time) else None
            rows.append({
                "job_id": j.job_id,
                "job_name": j.settings.name if j.settings else str(j.job_id),
                "run_id": r.run_id,
                "state": str(r.state.life_cycle_state) if r.state else "-",
                "result": str(r.state.result_state) if (r.state and r.state.result_state) else "-",
                "start_time": start,
                "duration_sec": dur,
                "trigger": str(r.trigger) if r.trigger else "-"
            })

import pandas as pd
df_logs = pd.DataFrame(rows)
print(f"Total run entries: {len(df_logs)}")
display(df_logs)

# COMMAND ----------

# DBTITLE 1,Resource Consumption per Job from System Tables
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- Resource Consumption: DBUs and USD cost per job (last 30 days)
# MAGIC -- Uses system.billing.usage + list_prices for accurate costing
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH job_defs AS (
# MAGIC   SELECT workspace_id, job_id, name AS job_name
# MAGIC   FROM system.lakeflow.jobs
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
# MAGIC ),
# MAGIC run_stats AS (
# MAGIC   SELECT workspace_id, job_id,
# MAGIC          COUNT(DISTINCT run_id) AS total_runs,
# MAGIC          SUM(CAST(period_end_time - period_start_time AS LONG)) / 3600 AS total_compute_hours,
# MAGIC          MAX_BY(result_state, period_end_time) FILTER (WHERE result_state IS NOT NULL) AS last_result
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_end_time > CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
# MAGIC   GROUP BY workspace_id, job_id
# MAGIC ),
# MAGIC job_cost AS (
# MAGIC   SELECT u.workspace_id,
# MAGIC          u.usage_metadata.job_id AS job_id,
# MAGIC          u.sku_name,
# MAGIC          SUM(u.usage_quantity) AS total_dbus,
# MAGIC          SUM(u.usage_quantity * pricing.`default`) AS total_cost_usd
# MAGIC   FROM system.billing.usage u
# MAGIC   LEFT JOIN system.billing.list_prices pricing
# MAGIC     ON u.sku_name = pricing.sku_name
# MAGIC    AND pricing.price_start_time <= u.usage_start_time
# MAGIC    AND (pricing.price_end_time >= u.usage_start_time OR pricing.price_end_time IS NULL)
# MAGIC    AND pricing.currency_code = 'USD'
# MAGIC   WHERE u.billing_origin_product = 'JOBS'
# MAGIC     AND u.usage_metadata.job_id IS NOT NULL
# MAGIC     AND u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT j.job_name,
# MAGIC        j.job_id,
# MAGIC        r.total_runs,
# MAGIC        ROUND(r.total_compute_hours, 2) AS compute_hours,
# MAGIC        c.sku_name,
# MAGIC        CASE
# MAGIC          WHEN UPPER(c.sku_name) LIKE '%SERVERLESS%' THEN 'Serverless'
# MAGIC          WHEN UPPER(c.sku_name) LIKE '%JOBS%' OR UPPER(c.sku_name) LIKE '%AUTOMATED%' THEN 'Job Cluster'
# MAGIC          ELSE 'All-Purpose'
# MAGIC        END AS cluster_type,
# MAGIC        ROUND(c.total_dbus, 2) AS total_dbus,
# MAGIC        ROUND(c.total_cost_usd, 2) AS cost_usd,
# MAGIC        r.last_result
# MAGIC FROM job_defs j
# MAGIC INNER JOIN run_stats r USING (workspace_id, job_id)
# MAGIC LEFT JOIN job_cost c USING (workspace_id, job_id)
# MAGIC ORDER BY c.total_cost_usd DESC NULLS LAST
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,TCO and ROI Comparison by Cluster Type
# ============================================================
# TCO & ROI Comparison: Serverless vs Job Cluster vs All-Purpose
# ============================================================
# Builds a comparison table for 5+ jobs showing:
#   - Actual DBU consumption and cost
#   - Estimated cost under each cluster type
#   - ROI (efficiency gain) when switching cluster types
# ============================================================

import pandas as pd

# Query actual billing data grouped by job and SKU category
tco_df = spark.sql("""
WITH job_defs AS (
  SELECT workspace_id, job_id, name AS job_name
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
),
run_stats AS (
  SELECT workspace_id, job_id,
         COUNT(DISTINCT run_id) AS total_runs,
         SUM(run_duration_seconds) AS total_duration_sec
  FROM system.lakeflow.job_run_timeline
  WHERE period_end_time > CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
    AND run_duration_seconds IS NOT NULL
  GROUP BY workspace_id, job_id
),
job_cost AS (
  SELECT u.workspace_id,
         u.usage_metadata.job_id AS job_id,
         u.sku_name,
         CASE
           WHEN UPPER(u.sku_name) LIKE '%SERVERLESS%' THEN 'Serverless'
           WHEN UPPER(u.sku_name) LIKE '%JOBS%' OR UPPER(u.sku_name) LIKE '%AUTOMATED%' THEN 'Job Cluster'
           ELSE 'All-Purpose'
         END AS cluster_type,
         SUM(u.usage_quantity) AS total_dbus,
         SUM(u.usage_quantity * pricing.`default`) AS total_cost_usd
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices pricing
    ON u.sku_name = pricing.sku_name
   AND pricing.price_start_time <= u.usage_start_time
   AND (pricing.price_end_time >= u.usage_start_time OR pricing.price_end_time IS NULL)
   AND pricing.currency_code = 'USD'
  WHERE u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL
    AND u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  GROUP BY ALL
)
SELECT j.job_name,
       j.job_id,
       r.total_runs,
       ROUND(r.total_duration_sec / 3600.0, 2) AS compute_hours,
       c.cluster_type AS actual_cluster_type,
       c.sku_name,
       ROUND(c.total_dbus, 4) AS actual_dbus,
       ROUND(c.total_cost_usd, 4) AS actual_cost_usd
FROM job_defs j
INNER JOIN run_stats r USING (workspace_id, job_id)
INNER JOIN job_cost c USING (workspace_id, job_id)
WHERE c.total_cost_usd > 0
ORDER BY c.total_cost_usd DESC
""").toPandas()

if tco_df.empty:
    print("No billing data found for the last 30 days. Displaying estimated TCO model instead.")
else:
    print(f"Found {len(tco_df)} job-cost records\n")

# Cast decimal columns to float for pandas operations
for col in ['actual_dbus', 'actual_cost_usd', 'compute_hours']:
    tco_df[col] = pd.to_numeric(tco_df[col], errors='coerce')

# ---- Build TCO/ROI comparison for top 5 jobs ----
# AWS list-price benchmarks (USD per DBU)
PRICE_SERVERLESS = 0.07    # Jobs Serverless Compute
PRICE_JOB_CLUSTER = 0.15   # Jobs Compute (classic)
PRICE_ALL_PURPOSE = 0.55   # All-Purpose Compute

top_jobs = tco_df.groupby(['job_name', 'job_id', 'total_runs', 'compute_hours', 'actual_cluster_type']).agg(
    actual_dbus=('actual_dbus', 'sum'),
    actual_cost_usd=('actual_cost_usd', 'sum')
).reset_index().nlargest(5, 'actual_cost_usd')

rows = []
for _, job in top_jobs.iterrows():
    dbus = job['actual_dbus']
    # Estimate cost under each cluster type
    cost_serverless   = round(dbus * PRICE_SERVERLESS, 4)
    cost_job_cluster  = round(dbus * PRICE_JOB_CLUSTER, 4)
    cost_all_purpose  = round(dbus * PRICE_ALL_PURPOSE, 4)
    actual_cost = round(job['actual_cost_usd'], 4)

    # ROI = savings relative to All-Purpose baseline
    roi_serverless  = round(((cost_all_purpose - cost_serverless) / cost_all_purpose) * 100, 1) if cost_all_purpose else 0
    roi_job_cluster = round(((cost_all_purpose - cost_job_cluster) / cost_all_purpose) * 100, 1) if cost_all_purpose else 0

    rows.append({
        'Job Name': job['job_name'][:45],
        'Runs (30d)': int(job['total_runs']),
        'Compute Hrs': job['compute_hours'],
        'Actual Type': job['actual_cluster_type'],
        'DBUs': round(dbus, 2),
        'Actual Cost ($)': actual_cost,
        'Serverless ($)': cost_serverless,
        'Job Cluster ($)': cost_job_cluster,
        'All-Purpose ($)': cost_all_purpose,
        'ROI Serverless (%)': f"{roi_serverless}%",
        'ROI Job Cluster (%)': f"{roi_job_cluster}%"
    })

result_df = pd.DataFrame(rows)
print("=" * 80)
print("  TCO & ROI COMPARISON — Top 5 Jobs by Cost (Last 30 Days)")
print("=" * 80)
print(f"\n  Benchmark prices (USD/DBU): Serverless=${PRICE_SERVERLESS} | Job Cluster=${PRICE_JOB_CLUSTER} | All-Purpose=${PRICE_ALL_PURPOSE}")
print(f"  ROI = savings vs All-Purpose baseline\n")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Run DAG Job
# ============================================================
# E3. Run the DAG job and monitor task-by-task
# ============================================================
# We trigger the job and watch each task progress through
# the DAG. The SDK's run_now().result() blocks until all
# tasks complete.
# ============================================================

print(f"Triggering DAG job: {DAG_JOB_NAME} (ID: {dag_job.job_id})")
print(f"Start time: {datetime.now().strftime('%H:%M:%S')}")
print("Waiting for all 4 tasks...\n")

try:
    dag_run = w.jobs.run_now(dag_job.job_id).result()
except Exception as e:
    print(f"Job finished with error: {e}")
    # Still fetch the run details for inspection
    runs = list(w.jobs.list_runs(job_id=dag_job.job_id, limit=1))
    dag_run = w.jobs.get_run(runs[0].run_id) if runs else None

print(f"{'Task':<15} {'State':<15} {'Result':<12} {'Duration':>10}")
print("-" * 55)

for task_run in sorted(dag_run.tasks, key=lambda t: t.start_time or 0):
    duration = ""
    if task_run.start_time and task_run.end_time:
        duration = f"{(task_run.end_time - task_run.start_time) / 1000:.1f}s"
    print(f"{task_run.task_key:<15} {str(task_run.state.life_cycle_state):<15} {str(task_run.state.result_state):<12} {duration:>10}")

print(f"\nOverall: {dag_run.state.result_state}")
print(f"Total duration: {(dag_run.end_time - dag_run.start_time) / 1000:.1f}s")

# COMMAND ----------

# DBTITLE 1,Observation E
# MAGIC %md
# MAGIC **Observation:** All four tasks executed in sequence. Notice how:
# MAGIC - Each task waited for its predecessor to complete before starting
# MAGIC - The total duration is the **sum** of all task durations (linear chain)
# MAGIC - In a fan-out pattern, parallel tasks would run simultaneously, reducing total time
# MAGIC - Serverless compute was provisioned independently for each task (no shared cluster)

# COMMAND ----------

# DBTITLE 1,Section F Intro
# MAGIC %md
# MAGIC ## F. Scheduling and Triggers
# MAGIC
# MAGIC ### What Happens Here
# MAGIC We configure the job to run on a **cron schedule** and explore other trigger types. In production, most ETL jobs run on a daily or hourly schedule.
# MAGIC
# MAGIC ### Trigger Types
# MAGIC
# MAGIC | Trigger | Use Case | Configuration |
# MAGIC |---|---|---|
# MAGIC | **Manual** | Testing, ad-hoc runs | `run_now()` call or UI button |
# MAGIC | **Cron Schedule** | Recurring ETL (daily, hourly) | Quartz cron expression |
# MAGIC | **File Arrival** | Process data when new files land | Volume path + wait time |
# MAGIC | **Continuous** | Always-on streaming jobs | Auto-restart on completion |
# MAGIC | **Table Update** | React to Delta table changes | Table name (trigger on new data) |
# MAGIC
# MAGIC ### Cron Expression Syntax
# MAGIC
# MAGIC ```
# MAGIC ┌──────────── second (0–59)
# MAGIC │ ┌────────── minute (0–59)
# MAGIC │ │ ┌──────── hour (0–23)
# MAGIC │ │ │ ┌────── day of month (1–31)
# MAGIC │ │ │ │ ┌──── month (1–12)
# MAGIC │ │ │ │ │ ┌── day of week (1–7, MON–SUN)
# MAGIC │ │ │ │ │ │
# MAGIC * * * * * *
# MAGIC ```
# MAGIC
# MAGIC | Expression | Meaning |
# MAGIC |---|---|
# MAGIC | `0 0 8 * * ?` | Daily at 8:00 AM |
# MAGIC | `0 0 */2 * * ?` | Every 2 hours |
# MAGIC | `0 30 6 ? * MON-FRI` | Weekdays at 6:30 AM |
# MAGIC | `0 0 0 1 * ?` | First day of every month at midnight |

# COMMAND ----------

# DBTITLE 1,Add Schedule to Job
# ============================================================
# F1. Add a cron schedule to the DAG job
# ============================================================
# We update the existing job to add a daily 8 AM schedule.
# The schedule uses the Quartz cron format.
# pause_status=PAUSED means the schedule is defined but not
# active — useful for testing before going live.
# ============================================================

w.jobs.update(
    job_id=dag_job.job_id,
    new_settings=JobSettings(
        name=DAG_JOB_NAME,
        schedule=CronSchedule(
            quartz_cron_expression="0 0 8 * * ?",  # Daily at 8 AM
            timezone_id="Asia/Kolkata",
            pause_status=PauseStatus.PAUSED          # Paused for lab safety
        )
    )
)

# Verify the schedule was applied
updated_job = w.jobs.get(dag_job.job_id)
schedule = updated_job.settings.schedule

print(f"Schedule added to job: {DAG_JOB_NAME}")
print(f"  Cron:      {schedule.quartz_cron_expression}")
print(f"  Timezone:  {schedule.timezone_id}")
print(f"  Status:    {schedule.pause_status} (safe for lab)")
print(f"\nTo activate in production, set pause_status=PauseStatus.UNPAUSED")

# COMMAND ----------

# DBTITLE 1,F2 File Arrival Trigger
# MAGIC %md
# MAGIC ### F2. File Arrival Trigger (Concept)
# MAGIC
# MAGIC File arrival triggers start a job when new files appear in a Unity Catalog Volume:
# MAGIC
# MAGIC ```python
# MAGIC # Example: trigger when new CSV files land in the Volume
# MAGIC w.jobs.update(
# MAGIC     job_id=job_id,
# MAGIC     new_settings=JobSettings(
# MAGIC         trigger=TriggerSettings(
# MAGIC             file_arrival=FileArrivalTriggerConfiguration(
# MAGIC                 url="/Volumes/dbacademy/labuser2/raw_data/csv",
# MAGIC                 min_time_between_triggers_seconds=300,  # 5-min cooldown
# MAGIC                 wait_after_last_change_seconds=60       # Wait 60s for more files
# MAGIC             )
# MAGIC         )
# MAGIC     )
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Description | Typical Value |
# MAGIC |---|---|---|
# MAGIC | `url` | Volume path to watch | `/Volumes/catalog/schema/volume/folder` |
# MAGIC | `min_time_between_triggers_seconds` | Minimum gap between runs | 300 (5 minutes) |
# MAGIC | `wait_after_last_change_seconds` | Wait for more files after last change | 60 (1 minute) |
# MAGIC
# MAGIC > **Note:** File arrival triggers only work with Unity Catalog Volumes (not DBFS).

# COMMAND ----------

# DBTITLE 1,Section G Intro
# MAGIC %md
# MAGIC ## G. Retry Policies and Error Handling
# MAGIC
# MAGIC ### What Happens Here
# MAGIC We configure **task-level retry policies** so that transient failures (network blips, temporary resource issues) are automatically recovered without human intervention.
# MAGIC
# MAGIC ### Retry Configuration
# MAGIC
# MAGIC | Parameter | Description | Recommended |
# MAGIC |---|---|---|
# MAGIC | `max_retries` | How many times to retry a failed task | 1–3 |
# MAGIC | `min_retry_interval_millis` | Minimum wait between retries | 60000 (1 min) |
# MAGIC | `retry_on_timeout` | Also retry if the task times out | `True` |
# MAGIC
# MAGIC ### Timeout Hierarchy
# MAGIC
# MAGIC ```
# MAGIC Job Timeout (overall)  ≥  Task Timeout (per task)
# MAGIC       1800s                    600s
# MAGIC ```
# MAGIC
# MAGIC If a task exceeds its timeout, it is killed. If the overall job timeout is reached, all remaining tasks are cancelled.

# COMMAND ----------

# DBTITLE 1,Update Job with Retries
# ============================================================
# G1. Update the DAG job with retry policies
# ============================================================
# We update each task to have a retry policy. In production,
# you'd set retries based on the task's failure mode:
# - Idempotent tasks: safe to retry (most ETL)
# - Non-idempotent tasks: use MERGE for safe retries
# ============================================================

# Get current job definition
current_settings = w.jobs.get(dag_job.job_id).settings

# Add retry policies and email notifications to each task
for task in current_settings.tasks:
    task.retry_on_timeout = True
    task.max_retries = 2
    task.min_retry_interval_millis = 60000  # 1 minute between retries
    task.timeout_seconds = 600              # 10-min per task

# Update the job
w.jobs.reset(
    job_id=dag_job.job_id,
    new_settings=current_settings
)

# Verify
updated = w.jobs.get(dag_job.job_id)
for task in updated.settings.tasks:
    print(f"Task: {task.task_key:<15} retries={task.max_retries}  timeout={task.timeout_seconds}s  retry_on_timeout={task.retry_on_timeout}")

print(f"\nAll tasks updated with retry policies!")

# COMMAND ----------

# DBTITLE 1,Section H Intro
# MAGIC %md
# MAGIC ## H. Email Notifications
# MAGIC
# MAGIC ### What Happens Here
# MAGIC We add **email notifications** so the team is alerted on job success, failure, or duration threshold breach.
# MAGIC
# MAGIC ### Notification Channels
# MAGIC
# MAGIC | Channel | Configuration | Use Case |
# MAGIC |---|---|---|
# MAGIC | **Email** | List of email addresses | Standard alerts |
# MAGIC | **Webhook** | URL for Slack/Teams/PagerDuty | ChatOps integration |
# MAGIC | **System** | Built-in (Job Events table) | Auditing and compliance |

# COMMAND ----------

# DBTITLE 1,Add Notifications
# ============================================================
# H1. Add email notifications to the job
# ============================================================
# Notifications can be set at the job level (all tasks) or
# per-task. Here we set job-level notifications.
# ============================================================

current_settings = w.jobs.get(dag_job.job_id).settings

current_settings.email_notifications = JobEmailNotifications(
    on_success=[current_user.user_name],    # Email on success
    on_failure=[current_user.user_name],    # Email on failure
    on_duration_warning_threshold_exceeded=[current_user.user_name]
)

# Duration warning: alert if job takes > 20 minutes
current_settings.health = JobsHealthRules(
    rules=[
        JobsHealthRule(
            metric=JobsHealthMetric.RUN_DURATION_SECONDS,
            op=JobsHealthOperator.GREATER_THAN,
            value=1200  # 20 minutes
        )
    ]
)

w.jobs.reset(job_id=dag_job.job_id, new_settings=current_settings)

print(f"Notifications configured for: {current_user.user_name}")
print(f"  On success:    Email")
print(f"  On failure:    Email")
print(f"  Duration > 20m: Email warning")

# COMMAND ----------

# DBTITLE 1,Section I Intro
# MAGIC %md
# MAGIC ## I. Job Run History and Diagnostics
# MAGIC
# MAGIC ### What Happens Here
# MAGIC We query the **run history** of our job to analyze execution patterns, durations, and success rates. This is essential for production monitoring and SLA tracking.
# MAGIC
# MAGIC ### Key APIs
# MAGIC
# MAGIC | API | Purpose |
# MAGIC |---|---|
# MAGIC | `w.jobs.list_runs()` | List all runs of a job |
# MAGIC | `w.jobs.get_run()` | Get details of a specific run |
# MAGIC | `w.jobs.get_run_output()` | Get task output (notebook exit value) |
# MAGIC | `w.jobs.cancel_run()` | Cancel a running job |
# MAGIC | `w.jobs.delete_run()` | Delete a run from history |

# COMMAND ----------

# DBTITLE 1,Query Run History
# ============================================================
# I1. Query run history for the DAG job
# ============================================================
# List all runs and display a summary table.
# In production, you'd use this for SLA dashboards
# and failure pattern analysis.
# ============================================================

runs = list(w.jobs.list_runs(job_id=dag_job.job_id, limit=10))

print(f"Run history for: {DAG_JOB_NAME}")
print(f"{'Run ID':<12} {'State':<14} {'Result':<10} {'Duration':>10} {'Start Time'}")
print("-" * 70)

for r in runs:
    duration = ""
    start_time = ""
    if r.start_time:
        start_time = datetime.fromtimestamp(r.start_time / 1000).strftime('%Y-%m-%d %H:%M')
    if r.start_time and r.end_time:
        duration = f"{(r.end_time - r.start_time) / 1000:.0f}s"
    
    result = str(r.state.result_state) if r.state.result_state else "RUNNING"
    print(f"{r.run_id:<12} {str(r.state.life_cycle_state):<14} {result:<10} {duration:>10} {start_time}")

print(f"\nTotal runs: {len(runs)}")

# COMMAND ----------

# DBTITLE 1,Query via System Tables
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- I2. Query job runs from system tables (production approach)
# MAGIC -- ============================================================
# MAGIC -- system.lakeflow.job_run_timeline provides a comprehensive
# MAGIC -- view of all job runs across the workspace.
# MAGIC -- This is the recommended approach for dashboards and auditing.
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT 
# MAGIC     job_id,
# MAGIC     run_id,
# MAGIC     result_state,
# MAGIC     run_duration_seconds,
# MAGIC     run_name,
# MAGIC     run_type
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE account_id IS NOT NULL
# MAGIC ORDER BY run_id DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Section J Intro
# MAGIC %md
# MAGIC ## J. Job Compute Configuration
# MAGIC
# MAGIC ### What Happens Here
# MAGIC We explore the three compute options for job tasks and demonstrate creating a job with a **dedicated job cluster** — useful for large Spark workloads that need custom cluster settings.
# MAGIC
# MAGIC ### Serverless vs Job Cluster Comparison
# MAGIC
# MAGIC | Feature | Serverless | Job Cluster |
# MAGIC |---|---|---|
# MAGIC | **Startup Time** | ~10 seconds | 2–4 minutes |
# MAGIC | **Configuration** | Zero (auto-managed) | Full control (node types, Spark config) |
# MAGIC | **Cost** | Per-second billing | Spot instances possible |
# MAGIC | **Auto-scaling** | Automatic | Configurable (min/max workers) |
# MAGIC | **Libraries** | Pre-installed (install via `%pip`) | Custom init scripts |
# MAGIC | **Best For** | Most tasks, SQL, lightweight Python | Large Spark jobs, ML training |
# MAGIC
# MAGIC ### When to Use Job Clusters
# MAGIC
# MAGIC - Workloads requiring **GPU instances** (ML training)
# MAGIC - Tasks needing **custom Spark configurations** (shuffle partitions, memory tuning)
# MAGIC - Cost optimization with **spot instances** for long-running jobs
# MAGIC - Tasks requiring **specific DBR versions** or **init scripts**

# COMMAND ----------

# DBTITLE 1,Create Job with Job Cluster
# ============================================================
# J1. Create a job with a dedicated job cluster (example)
# ============================================================
# Job clusters are created when the job starts and terminated
# after completion. They are cost-optimized for production.
#
# NOTE: This cell demonstrates the configuration but does NOT
# run the job (to avoid unnecessary compute costs in the lab).
# ============================================================

JOB_CLUSTER_JOB_NAME = f"Lab5_JobCluster_Demo_{current_user.user_name.split('@')[0]}"

# Clean up previous
for j in w.jobs.list(name=JOB_CLUSTER_JOB_NAME):
    w.jobs.delete(j.job_id)

job_cluster_job = w.jobs.create(
    name=JOB_CLUSTER_JOB_NAME,
    # Define a reusable job cluster (shared across tasks)
    job_clusters=[
        JobCluster(
            job_cluster_key="etl_cluster",
            new_cluster=ClusterSpec(
                spark_version="15.4.x-scala2.12",    # Latest LTS runtime
                num_workers=2,                         # Fixed 2-worker cluster
                node_type_id="i3.xlarge",              # Instance type
                spark_conf={
                    "spark.sql.shuffle.partitions": "200",
                    "spark.databricks.delta.optimizeWrite.enabled": "true"
                },
                custom_tags={"lab": "lab5", "team": "data-engineering"}
            )
        )
    ],
    tasks=[
        Task(
            task_key="etl_with_cluster",
            description="ETL task running on dedicated job cluster",
            job_cluster_key="etl_cluster",    # Reference the cluster above
            notebook_task=NotebookTask(
                notebook_path=f"{base_path}/_task_ingest",
                base_parameters=common_params
            )
        )
    ],
    tags={"lab": "lab5", "compute": "job-cluster"}
)

print(f"Job cluster demo created!")
print(f"  Job ID:       {job_cluster_job.job_id}")
print(f"  Cluster:      etl_cluster (i3.xlarge, 2 workers)")
print(f"  Runtime:      15.4.x-scala2.12")
print(f"  Spark Config: shuffle.partitions=200, optimizeWrite=true")
print(f"\n  (Not running — job cluster demo for reference only)")

# COMMAND ----------

# DBTITLE 1,Section K Intro
# MAGIC %md
# MAGIC ## K. Cleanup
# MAGIC
# MAGIC Remove the lab jobs and task notebooks to keep the workspace tidy. The Bronze/Silver/Gold tables from earlier labs are preserved.

# COMMAND ----------

# DBTITLE 1,Cleanup Lab Resources
# ============================================================
# K. Clean up lab-created jobs and task notebooks
# ============================================================

# Delete jobs
for name in [JOB_NAME, DAG_JOB_NAME, JOB_CLUSTER_JOB_NAME]:
    for j in w.jobs.list(name=name):
        w.jobs.delete(j.job_id)
        print(f"Deleted job: {name} (ID: {j.job_id})")

# Delete task notebooks
for nb in ["_task_validate_bronze", "_task_ingest.py", "_task_transform.py", "_task_aggregate.py"]:
    path = f"{base_path}/{nb}"
    try:
        w.workspace.delete(path)
        print(f"Deleted notebook: {path}")
    except:
        pass

print("\nCleanup complete! All lab jobs and task notebooks removed.")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary — Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC | Section | Topic | Key Skill |
# MAGIC |---|---|---|
# MAGIC | **A** | Environment Setup | SDK installation and WorkspaceClient initialization |
# MAGIC | **B** | Understanding Jobs | Job components, task types, compute options |
# MAGIC | **C** | Single-Task Job | `w.jobs.create()` with notebook task and parameters |
# MAGIC | **D** | Running & Monitoring | `run_now().result()`, task-level output inspection |
# MAGIC | **E** | Multi-Task DAG | `depends_on` for task chaining, linear pipeline pattern |
# MAGIC | **F** | Scheduling | Cron schedules, file arrival triggers, pause/unpause |
# MAGIC | **G** | Retry Policies | `max_retries`, `retry_on_timeout`, timeout hierarchy |
# MAGIC | **H** | Notifications | Email alerts on success/failure/duration threshold |
# MAGIC | **I** | Run History | SDK run listing + system table queries for auditing |
# MAGIC | **J** | Compute Config | Serverless vs job cluster trade-offs |
# MAGIC
# MAGIC ### Production Best Practices
# MAGIC
# MAGIC 1. **Use serverless compute** unless you need custom Spark config or GPUs
# MAGIC 2. **Parameterize everything** — use `dbutils.widgets` and `base_parameters` for environment flexibility
# MAGIC 3. **Set retry policies** — 2 retries with 1-minute intervals handles most transient failures
# MAGIC 4. **Use MERGE for idempotency** — safe to re-run tasks without creating duplicates (Lab 4)
# MAGIC 5. **Monitor via system tables** — `system.lakeflow.job_run_timeline` for dashboards and SLA tracking
# MAGIC 6. **Tag everything** — `tags` on jobs help with cost attribution and organization
# MAGIC 7. **Pause schedules during development** — use `PauseStatus.PAUSED` until ready for production
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 6 (Pipelines):** Declarative ETL with Lakeflow Spark Declarative Pipelines
# MAGIC - **Lab 7 (Governance):** Unity Catalog permissions, masking, and row-level security