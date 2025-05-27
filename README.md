# GBS-Data-Pipeline-A-Data-Engineering-Project-using-PySpark-Databricks
Understanding Guillain-Barré Syndrome through Medical Research & Public Search Insights
This end-to-end data engineering project aims to understand Guillain-Barré Syndrome (GBS) by integrating structured clinical trial data with dynamic Google search insights. It blends emotional motivation with technical execution — inspired by a personal journey and driven by data.

✨ **Using PySpark on Databricks**  
I developed a real-time pipeline that fetches, processes, and analyzes two powerful datasets:  
✔ ClinicalTrials.gov API – structured data on medical research studies  
✔ Google Search results via SerpApi – to understand how public content portrays GBS  

✨ **Tech Stack**  
PySpark, Databricks, Spark SQL, Pandas, SerpApi, ClinicalTrials.gov API  

✨ **Approach**  
✔ Data Collection: Accessed clinical trial data using the ClinicalTrials.gov API and collected real-time Google search snippets using SerpApi.  
✔ Data Transformation: Converted raw JSON responses into Pandas DataFrames for initial transformation, then transferred and structured the cleaned data into Spark DataFrames on Databricks.  
✔ Data Cleaning: Removed nulls, duplicates, and irrelevant fields, standardized columns, and refined textual content for analysis.  
✔ Exploratory Data Analysis (EDA): Analyzed trial statuses (Recruiting, Completed, etc.), checked for published results, and categorized search snippets into Symptoms, Causes, Treatment, Recovery, Diagnosis, and Others.  
✔ Insight Generation: Compared how GBS is portrayed in scientific research versus dynamic online content, highlighting differences in focus and source credibility.  

✨ **Key Objectives**  
✔ Analyze GBS clinical trials: activity status and result availability.  
✔ Examine and classify Google search content into medically relevant categories.  
✔ Compare structured research insights with public-facing online information.  

✨ **Key Insights**  
✔ Only 5.26% of GBS clinical trials had published results, indicating limited public access to medical outcomes.  
✔ Symptoms and Causes dominate Google search snippets, while Treatment and Recovery are less frequently mentioned.  
✔ Reputed sources like CDC, Mayo Clinic, and The Lancet consistently rank highest in search results.  
✔ Some article titles repeatedly appear in top positions, reflecting strong source credibility.  

✨ This project not only enhanced my data engineering skills but also helped me process a deeply personal experience. It’s a heartfelt combination of empathy, analysis, and real-world data application.




