# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### DQ Monitoring — Event Log View Setup
# MAGIC Creates the `event_log_raw` view for each pipeline
# MAGIC **One real constraint:** the `event_log` table-valued function
# MAGIC "cannot be used... to access the event logs of multiple pipelines."
# MAGIC
# MAGIC Two pipelines,
# MAGIC two separate views — `ingestion_event_log_raw` and `transformation_event_log_raw`
# MAGIC
# MAGIC Only the pipeline owner can create this view, and it can't be shared with other
# MAGIC users — run this as whichever identity owns both pipelines.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Target catalog")
dbutils.widgets.text("ingestion_pipeline_id", "10859f27-e167-4c7d-8df3-a09de5243ffe", "stepright-ingestion-pipeline ID")
dbutils.widgets.text("transformation_pipeline_id", "10ef85b4-0f3e-4084-9c7a-90a2add5d280", "stepright-transformation-pipeline ID")

catalog = dbutils.widgets.get("catalog")
ingestion_pipeline_id = dbutils.widgets.get("ingestion_pipeline_id")
transformation_pipeline_id = dbutils.widgets.get("transformation_pipeline_id")

if not ingestion_pipeline_id or not transformation_pipeline_id:
    raise ValueError(
        "Both pipeline IDs are required. Find them in each pipeline's Settings panel "
        "in the Lakeflow Pipelines Editor — this notebook won't guess them."
    )

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.stepright.ingestion_event_log_raw AS
SELECT * FROM event_log("{ingestion_pipeline_id}")
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.stepright.transformation_event_log_raw AS
SELECT * FROM event_log("{transformation_pipeline_id}")
""")

print(f"Created {catalog}.stepright.ingestion_event_log_raw")
print(f"Created {catalog}.stepright.transformation_event_log_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC dev.stepright.ingestion_event_log_raw
# MAGIC where event_type='flow_definition';