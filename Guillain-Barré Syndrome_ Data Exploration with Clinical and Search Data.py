# Databricks notebook source
# MAGIC %md
# MAGIC **PROJECT DESCRIPTION**
# MAGIC
# MAGIC This project explores data related to Guillain-Barré Syndrome from two sources clinical research data and search engine results. Using PySpark, I perform data cleaning, transformation, and exploratory analysis to understand patterns and insights from each dataset independently.

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetching and Setting Up Data from Clinical and SerpApi APIs**

# COMMAND ----------

# Fetching Clinical Trials Data for Guillain-Barré Syndrome using Clinicaltrials.gov API

import requests

# URL and parameters
url = "https://clinicaltrials.gov/api/v2/studies"
params = {
    "query.cond": "Guillain-Barre",
    "pageSize": "100"
}

# To store all results
all_studies = []  

while True:
    # API request
    response = requests.get(url, params=params)
    data = response.json()
    
    # Append current page studies
    all_studies.extend(data.get("studies", []))
    
    # Check for next page
    next_token = data.get("nextPageToken")
    if not next_token:
        break
    
    # Set token for next page
    params["pageToken"] = next_token

# Store full JSON response
clinical_data = {"studies": all_studies}

# Display the results in JSON format
display(clinical_data)


# COMMAND ----------

# Fetching Google Search Results for Guillain-Barré Syndrome using SerpApi

#%pip install google-search-results==2.4.2

from serpapi import GoogleSearch
import json

api_key = "1e9e3313e442d407a5bb0f40096ff8633cfd38190089299db0626007b6c08632"

# To store all results
fetched_results_raw = []

page_size = 10
max_pages = 10

# Loop over pages to fetch results
for page in range(max_pages):
    params = {
        "api_key": api_key,
        "engine": "google",
        "q": "Guillain Barre Syndrome",
        "location": "India",
        "google_domain": "google.co.in",
        "gl": "in",
        "hl": "hi",
        "cr": "countryUS",
        "start": page * page_size
    }

    search = GoogleSearch(params)
    results = search.get_dict()

    fetched_results_raw += results.get("organic_results", [])

# Display the results in JSON format
print(json.dumps(fetched_results_raw, indent=2, ensure_ascii=False))


# COMMAND ----------

# MAGIC %md
# MAGIC **Convert Clinical Trials and SerpApi JSON Data to Pandas DataFrames (Normalization)**

# COMMAND ----------

# Convert Clinical Trials JSON Data to Pandas DataFrame for Analysis

import requests
import pandas as pd

url = "https://clinicaltrials.gov/api/v2/studies"
params = {
    "query.cond": "Guillain-Barre",
    "pageSize": "100"  
}

response = requests.get(url, params=params)
data = response.json()

studies = data.get("studies", [])

# Creating an empty list to collect trial records
trials_list = []

# Loop through each study
for study in studies:
    try:
        id_module = study.get('protocolSection', {}).get('identificationModule', {})
        status_module = study.get('protocolSection', {}).get('statusModule', {})

        trials_list.append({
            "nctId": id_module.get('nctId', ''),
            "briefTitle": id_module.get('briefTitle', ''),
            "overallStatus": status_module.get('overallStatus', ''),
            "hasResults": study.get('hasResults', False)
        })

    except Exception as e:
        print(f"Error processing a study: {e}")

# Convert to DataFrame
df = pd.DataFrame(trials_list)

# Display the DataFrame
display(df.head(100))




# COMMAND ----------

# Convert Google Search Results JSON Data to Pandas DataFrame for Analysis


import pandas as pd
print(f"Length of fetched_results_raw: {len(fetched_results_raw)}")


try:
    serpapi_info = []

    for result in fetched_results_raw:
        serpapi_info.append({
            'Position': result.get('position', ''),
            'Title': result.get('title', ''),
            'Link': result.get('link', ''),
            'Snippet': result.get('snippet', ''),
            'Source': result.get('source', ''),
            'Displayed Link': result.get('displayed_link', ''),
            'Sitelinks': str(result.get('sitelinks', ''))  
        })

    df_serpapi = pd.DataFrame(serpapi_info)
    display(df_serpapi.head(100))

except NameError:
    print("Variable 'fetched_results_raw' not found. Please run the fetching cell first.")




# COMMAND ----------

# MAGIC %md
# MAGIC **Defining PySpark Schemas and Creating Separate DataFrames for Clinical Trials and Google Search Results**

# COMMAND ----------

# Defining PySpark Schema and Creating Clinical Trials DataFrame
from pyspark.sql.types import *

clinical_schema = StructType([
    StructField("nctId", StringType(), True),
    StructField("briefTitle", StringType(), True),
    StructField("overallStatus", StringType(), True),
    StructField("hasResults", BooleanType(), True)
])

df_clinical_spark = spark.createDataFrame(trials_list, schema=clinical_schema)
display(df_clinical_spark)


# COMMAND ----------

# Defining PySpark Schema and Creating Google Search Results DataFrame

from pyspark.sql.types import *

serpapi_schema = StructType([
    StructField("Position", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Link", StringType(), True),
    StructField("Snippet", StringType(), True),
    StructField("Source", StringType(), True),
    StructField("Displayed Link", StringType(), True),
    StructField("Sitelinks", StringType(), True)
])

df_serpapi_spark = spark.createDataFrame(df_serpapi, schema=serpapi_schema)
display(df_serpapi_spark)


# COMMAND ----------

# MAGIC %md
# MAGIC **Cleaning and Preparation of Clinical Trials Data**

# COMMAND ----------

# Clinical Trials Data Summary Statistics
display(df_clinical_spark.describe())

# COMMAND ----------

# Print Schema of Clinical Trials DataFrame
df_clinical_spark.printSchema()

# COMMAND ----------

# Check for Null Values in Clinical Trials DataFrame Columns

import pyspark.sql.functions as p
df_clinical_spark.select([
    p.count(p.when(p.col(c).isNull(), c)).alias(c) for c in df_clinical_spark.columns
]).display()


# COMMAND ----------

# Check for Duplicate Rows in Clinical Trials DataFrame

print("Duplicates in Clinical Data:")
df_clinical_spark.count() - df_clinical_spark.dropDuplicates().count()


# COMMAND ----------

# MAGIC %md
# MAGIC **Cleaning and Preparation of Google Search Results Data**

# COMMAND ----------

#Statistics Summary of Google Search Results Data
display(df_serpapi_spark.describe())

# COMMAND ----------

# Print Schema of Google Search Results DataFrame
df_serpapi_spark.printSchema()


# COMMAND ----------

# Check for Null Values in Google Search Results DataFrame

import pyspark.sql.functions as p

df_serpapi_spark.select([
    p.count(p.when(p.col(c).isNull(), c)).alias(c) for c in df_serpapi_spark.columns
]).display()


# COMMAND ----------

# Check for Duplicates in Google Search Results DataFrame

print("Duplicates in SERP API Data:")
df_serpapi_spark.count() - df_serpapi_spark.dropDuplicates().count()


# COMMAND ----------

# Identify and Display Duplicate Rows in Google Search Results DataFrame

from pyspark.sql import functions as p

# Group by all columns and count occurrences
duplicate_rows = (
    df_serpapi_spark
    .groupBy(df_serpapi_spark.columns)
    .count()
    .filter("count > 1")
)

# Show the duplicate rows
duplicate_rows.display(truncate=False)


# COMMAND ----------

# Remove Duplicate Rows from Google Search Results DataFrame and Report Count

before = df_serpapi_spark.count()
df_serpapi_spark = df_serpapi_spark.dropDuplicates()
after = df_serpapi_spark.count()

print(f"Removed {before - after} duplicate rows")


# COMMAND ----------

# Fill Missing Values in 'Sitelinks' Column of Google Search Results DataFrame

df_serpapi_spark = df_serpapi_spark.na.fill({'Sitelinks': 'No Sitelinks'})

# COMMAND ----------

# Identify Duplicate Rows in Google Search Results DataFrame

df_serpapi_spark.groupBy(df_serpapi_spark.columns) \
    .count().filter("count > 1").display()


# COMMAND ----------

# Count Null Values in Each Column of Google Search Results DataFrame

import pyspark.sql.functions as p

df_serpapi_spark.select([
    p.count(p.when(p.col(c).isNull(), c)).alias(c) for c in df_serpapi_spark.columns
]).display()


# COMMAND ----------

# Clean column names for df_serpapi_spark
df_serpapi_spark = df_serpapi_spark.withColumnRenamed("Displayed Link", "Displayed_Link")

# COMMAND ----------

# Display Cleaned Google Search Results DataFrame

display(df_serpapi_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC **Exploratory Data Analysis (EDA) of Clinical Trials and Google Search Data**

# COMMAND ----------

# MAGIC %md
# MAGIC **Distribution of Study Status in Clinical Trials (overallStatus)**

# COMMAND ----------

# Understand the distribution of studies by status — e.g., completed, recruiting, withdrawn, etc.

df_clinical_spark.groupBy("overallStatus") \
    .count() \
    .orderBy("count", ascending=False) \
    .display(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC **Percentage of studies with results vs without results**

# COMMAND ----------

# Quick insight into how many studies have published results — and whether most clinical trials on GBS report their findings
from pyspark.sql.functions import col

total_studies = df_clinical_spark.count()
results_group = df_clinical_spark.groupBy("hasResults").count()

results_group.withColumn(
    "percentage", 
    (col("count") / total_studies * 100).cast("decimal(5,2)")
).display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Filter studies with results for focused analysis**

# COMMAND ----------

# Filter and preview studies that have published results for further analysis
df_with_results = df_clinical_spark.filter(col("hasResults") == True)
print(f"Studies with results: {df_with_results.count()}")
df_with_results.display(5, truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC **Exploratory Data Analysis of Google Search Results**

# COMMAND ----------

# MAGIC %md
# MAGIC **Source-wise Result Count with Best Position**

# COMMAND ----------

#Source-wise count of Google Search results and their best positions

from pyspark.sql.functions import col, count, min

df_serpapi_spark.groupBy("Source") \
    .agg(
        count("*").alias("No_of_Results"),
        min("Position").alias("Best_Position")  # Lower means higher rank
    ) \
    .orderBy(col("No_of_Results").desc(), col("Best_Position")) \
    .display(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC **Which Titles appear most frequently in the top 3 positions, and which sources do they come from**

# COMMAND ----------

# Most frequent Titles in top 3 positions along with their Sources

from pyspark.sql.functions import col, count, collect_set

# Filter top 3 positions
top_positions_df = df_serpapi_spark.filter(col("Position") <= 3)

# Group by Title and collect the Sources
top_titles_with_sources = top_positions_df.groupBy("Title") \
    .agg(
        count("*").alias("Count"),
        collect_set("Source").alias("Top_Sources")
    ) \
    .orderBy(col("Count").desc())

top_titles_with_sources.display(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC **Topic Categorization of Google Search Snippets**

# COMMAND ----------

# Categorize search result snippets into medical topics like Symptoms, Causes, Treatment, etc. and count occurrences

from pyspark.sql.functions import when, col

df_tagged = df_serpapi_spark.withColumn("Topic",
    when(col("Snippet").rlike("(?i)symptom"), "Symptoms")
    .when(col("Snippet").rlike("(?i)cause|trigger"), "Causes")
    .when(col("Snippet").rlike("(?i)treat|therapy|manage"), "Treatment")
    .when(col("Snippet").rlike("(?i)recover|rehab"), "Recovery")
    .when(col("Snippet").rlike("(?i)diagnos"), "Diagnosis")
    .otherwise("Other")
)

df_tagged.groupBy("Topic").count().orderBy("count", ascending=False).display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Saving Clinical Trials and Google Search Results DataFrames as Spark Tables**

# COMMAND ----------

#Saving Clinical Trials DataFrame as a Spark Table
df_clinical_spark.write.mode("overwrite").saveAsTable("final_clinical_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final_clinical_data

# COMMAND ----------

#Saving Google Search Results DataFrame as a Spark Table
df_serpapi_spark.write.mode("overwrite").saveAsTable("final_serpapi_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final_serpapi_data

# COMMAND ----------

# MAGIC %md
# MAGIC **Derived Results Summary**

# COMMAND ----------

# MAGIC %md
# MAGIC **Clinical Trials Data Insights:**
# MAGIC
# MAGIC - The majority of clinical trials related to Guillain-Barré Syndrome (GBS) are completed (31 studies), while a significant portion remains unknown or recruiting.
# MAGIC
# MAGIC - Only about 5.26% of the studies have published results, indicating that most clinical trials on GBS have yet to report their findings.
# MAGIC
# MAGIC - A filtered subset of studies with results was created for focused analysis, highlighting the available data for deeper examination.
# MAGIC
# MAGIC **SERP API Search Results Insights:**
# MAGIC
# MAGIC - The most frequent sources in Google search results include authoritative health organizations such as the Centers for Disease Control and Prevention (CDC), Mayo Clinic, and The Lancet, often appearing at top ranks.
# MAGIC
# MAGIC - Titles related to Guillain-Barré Syndrome appearing in the top 3 search positions are mostly from reputable medical institutions, emphasizing the dominance of credible sources in search results.
# MAGIC
# MAGIC - Search snippets are mainly focused on Symptoms, followed by Causes and Treatment, suggesting public interest and information availability centers on understanding and managing the condition.

# COMMAND ----------

# MAGIC %md
# MAGIC