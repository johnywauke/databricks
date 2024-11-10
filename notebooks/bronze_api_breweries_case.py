# Databricks notebook source
# MAGIC %md
# MAGIC #BEES Data Engineering – Breweries Case
# MAGIC
# MAGIC **Objective:** The goal of this test is to assess your skills in consuming data from an API, transforming and persisting it into a data lake following the medallion architecture with three layers: raw data, curated data partitioned by location, and an analytical aggregated layer.
# MAGIC
# MAGIC Instructions:
# MAGIC 1. API: Use the Open Brewery DB API to fetch data. The API has an endpoint for listing breweries:
# MAGIC <https://api.openbrewerydb.org/breweries>
# MAGIC
# MAGIC 2. Orchestration Tool: Choose the orchestration tool of your preference (Airflow, Luigi, Mage etc.) to build a data pipeline. We're interested in seeing your ability to handle scheduling, retries, and error handling in the pipeline.
# MAGIC
# MAGIC 3. Language: Use the language of your preference for the requests and data transformation.
# MAGIC Please include test cases for your code. Python and PySpark are preferred but not mandatory.
# MAGIC
# MAGIC 4. Containerization: If you use Docker or Kubernetes for modularization, you'll earn extra points.
# MAGIC
# MAGIC 5. Data Lake Architecture: Your data lake must follow the medallion architecture having a bronze, silver, and gold layer:
# MAGIC
# MAGIC    a. Bronze Layer: Persist the raw data from the API in its native format or any format you find suitable.
# MAGIC
# MAGIC    b. Silver Layer: Transform the data to a columnar storage format such as parquet or delta, and partition it by brewery location. Please explain any other transformations you perform.
# MAGIC
# MAGIC    c. Gold Layer: Create an aggregated view with the quantity of breweries per type and location.
# MAGIC
# MAGIC 6. Monitoring/Alerting: Describe how you would implement a monitoring and alerting process for this pipeline. Consider data quality issues, pipeline failures, and other potential problems in your response.
# MAGIC
# MAGIC 7. Repository: Create a public repository on GitHub with your solution. Document your design choices, trade-offs, and provide clear instructions on how to run your application.
# MAGIC
# MAGIC 8. Cloud Services: If your solution requires any cloud services, please provide instructions on how to set them up. Please do not post them in your public repository.
# MAGIC
# MAGIC Evaluation Criteria:
# MAGIC Your solution will be evaluated based on the following criteria:
# MAGIC 1. Code Quality
# MAGIC 2. Solution Design
# MAGIC 3. Efficiency
# MAGIC 4. Completeness
# MAGIC 5. Documentation
# MAGIC 6. Error Handling
# MAGIC
# MAGIC Time Frame:
# MAGIC Please complete the test within 1 week and share the link to your GitHub repository with us.
# MAGIC Remember, the goal of this test is to showcase your skills and approach to building a data pipeline. Good
# MAGIC luck!

# COMMAND ----------

# MAGIC %md
# MAGIC # bronze.api.breweries_case

# COMMAND ----------

# MAGIC %md
# MAGIC ## Version
# MAGIC
# MAGIC | Enginner Name | Version | O.B.S. | Date (dd/mm/yyyy) |
# MAGIC |---------------|---------|--------|-------------------|
# MAGIC | Johny Wauke | 0.1 | TEST First version of pipeline | 09/11/2024 |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configs

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.streaming import * 
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import col
import requests, json, pandas as pd

# COMMAND ----------

providername = "api"
tablename = "breweries_case"
tableFullName = f"bronze.{providername}.{tablename}"

print("Table full name:", tableFullName)

# COMMAND ----------

# MAGIC %md
# MAGIC #Extract

# COMMAND ----------

result = []

#wauke: credentials = dbutils.secrets.get("") #wauke: api não precisa de credencial
r = requests.get(f'https://api.openbrewerydb.org/breweries')  #wauke: se precisar de credencial, headers={'Authorization': credentials}
result = r.json()

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformations

# COMMAND ----------

df = pd.json_normalize(result)
df = spark.createDataFrame(df)

# COMMAND ----------

# raw data without transformations and additional data schema to avoid error
df_final = (
    df_spark
    .withColumn("id", df_spark.id.cast(StringType()))
    .withColumn("name", df_spark.name.cast(StringType()))
    .withColumn("brewery_type", df_spark.brewery_type.cast(StringType()))
    .withColumn("state", df_spark.state.cast(StringType()))
    .withColumn("street", df_spark.street.cast(StringType()))
    .withColumn("address_1", df_spark.address_1.cast(StringType()))
    .withColumn("address_2", df_spark.address_2.cast(StringType()))
    .withColumn("address_3", df_spark.address_3.cast(StringType()))
    .withColumn("city", df_spark.city.cast(StringType()))
    .withColumn("state_province", df_spark.state_province.cast(StringType()))
    .withColumn("postal_code", df_spark.postal_code.cast(StringType()))
    .withColumn("country", df_spark.country.cast(StringType()))
    .withColumn("longitude", df_spark.longitude.cast(StringType()))
    .withColumn("latitude", df_spark.latitude.cast(StringType()))
    .withColumn("phone", df_spark.phone.cast(StringType()))
    .withColumn("website_url", df_spark.website_url.cast(StringType()))
    .withColumn("pk_historic", xxhash64(concat_ws("||", *df_spark.columns)))
    .withColumn("insertion_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load

# COMMAND ----------

df_final.write.mode("append").saveAsTable(tableFullName)
