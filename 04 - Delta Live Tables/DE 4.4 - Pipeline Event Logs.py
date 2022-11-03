# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring the Pipeline Events Logs
# MAGIC 
# MAGIC DLT uses the event logs to store much of the important information used to manage, report, and understand what's happening during pipeline execution.
# MAGIC 
# MAGIC Below, we provide a number of useful queries to explore the event log and gain greater insight into your DLT pipelines.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-04.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Event Log
# MAGIC The event log is managed as a Delta Lake table with some of the more important fields stored as nested JSON data.
# MAGIC 
# MAGIC The query below shows how simple it is to read this table and created a DataFrame and temporary view for interactive querying.

# COMMAND ----------

event_log_path = f"{DA.paths.storage_location}/system/events"

event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

display(event_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Latest Update ID
# MAGIC 
# MAGIC In many cases, you may wish to gain updates about the latest update (or the last N updates) to your pipeline.
# MAGIC 
# MAGIC We can easily capture the most recent update ID with a SQL query.

# COMMAND ----------

latest_update_id = spark.sql("""
    SELECT origin.update_id
    FROM event_log_raw
    WHERE event_type = 'create_update'
    ORDER BY timestamp DESC LIMIT 1""").first().update_id

print(f"Latest Update ID: {latest_update_id}")

# Push back into the spark config so that we can use it in a later query.
spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Audit Logging
# MAGIC 
# MAGIC Events related to running pipelines and editing configurations are captured as **`user_action`**.
# MAGIC 
# MAGIC Yours should be the only **`user_name`** for the pipeline you configured during this lesson.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'user_action'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Lineage
# MAGIC 
# MAGIC DLT provides built-in lineage information for how data flows through your table.
# MAGIC 
# MAGIC While the query below only indicates the direct predecessors for each table, this information can easily be combined to trace data in any table back to the point it entered the lakehouse.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'flow_definition' AND 
# MAGIC       origin.update_id = '${latest_update.id}'

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH l1_raw AS (
# MAGIC   SELECT 
# MAGIC     details:flow_definition.output_dataset,
# MAGIC     details:flow_definition.input_datasets
# MAGIC   FROM event_log_raw 
# MAGIC   WHERE event_type = 'flow_definition' AND 
# MAGIC         origin.update_id = '${latest_update.id}'
# MAGIC ),
# MAGIC l1_arr AS (
# MAGIC   SELECT
# MAGIC     output_dataset,
# MAGIC     regexp_extract_all(input_datasets, "(\")([^\"]+)(\")", 2) AS input_datasets
# MAGIC   FROM l1_raw
# MAGIC ),
# MAGIC l1 AS (
# MAGIC   SELECT
# MAGIC     l1_arr.output_dataset,
# MAGIC     1 AS input_depth,
# MAGIC     explode(l1_arr.input_datasets) AS input_dataset
# MAGIC   FROM l1_arr
# MAGIC ),
# MAGIC l2 AS (
# MAGIC   SELECT
# MAGIC     l1.output_dataset,
# MAGIC     2 AS input_depth,
# MAGIC     l2.input_dataset
# MAGIC   FROM l1
# MAGIC   LEFT JOIN l1 AS l2
# MAGIC     ON l1.input_dataset = l2.output_dataset
# MAGIC   WHERE l2.input_dataset IS NOT NULL
# MAGIC ),
# MAGIC l3 AS (
# MAGIC   SELECT
# MAGIC     l2.output_dataset,
# MAGIC     3 AS input_depth,
# MAGIC     l3.input_dataset
# MAGIC   FROM l2
# MAGIC   LEFT JOIN l1 AS l3
# MAGIC     ON l2.input_dataset = l3.output_dataset
# MAGIC   WHERE l3.input_dataset IS NOT NULL
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM (
# MAGIC   SELECT * FROM l1
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM l2
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM l3
# MAGIC )
# MAGIC PIVOT 
# MAGIC   (COLLECT_SET(input_dataset) FOR input_depth IN (1 AS L1,2 AS L2,3 AS L3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Data Quality Metrics
# MAGIC 
# MAGIC Finally, data quality metrics can be extremely useful for both long term and short term insights into your data.
# MAGIC 
# MAGIC Below, we capture the metrics for each constraint throughout the entire lifetime of our table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_expectations.dataset as dataset,
# MAGIC        row_expectations.name as expectation,
# MAGIC        SUM(row_expectations.passed_records) as passing_records,
# MAGIC        SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (SELECT explode(
# MAGIC             from_json(details :flow_progress :data_quality :expectations,
# MAGIC                       "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
# MAGIC           ) row_expectations
# MAGIC    FROM event_log_raw
# MAGIC    WHERE event_type = 'flow_progress' AND 
# MAGIC          origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY row_expectations.dataset, row_expectations.name

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
