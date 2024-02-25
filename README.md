# NYC_Food_Inspection_Data_Modeling

This project outlines the data processing workflow for ingesting and transforming data from an API, performing inspections, and storing the results in Parquet tables. The entire process is orchestrated and managed through Airflow(Cloud Composer) on Google Cloud Platform (GCP).

## Data Flow:

Data Acquisition:
  >> Data is retrieved from an external API.
  >> Python and PySpark are used to process and prepare the data for further processing.
  
## Raw Data Storage:
  >> The processed data is temporarily saved in a Google Cloud Storage (GCS) bucket.
  >> GCS provides a scalable and cost-effective storage solution for large datasets.

## Data Processing:
  Data is read from the GCS bucket.

  Dimension Tables:
  >> Data is used to create dimension tables, which provide context and meaning to fact tables. These tables store descriptive attributes about entities in your data model.
  
  Fact Tables:
  >> Food Inspection: The data is processed to create fact food inspection tables. These tables store detailed information about food inspections, such as inspection dates, results, and violations.
  >> Violation: Data is processed to create a fact violation table. These tables store specific violations found during inspections, along with details like violation types and severity levels.

## Data Storage:
  >> The processed data is stored in Parquet format, a columnar data format optimized for fast querying and efficient data compression. Parquet is well-suited for data warehouses and analytics due to its performance benefits.
  
## Orchestration and Management:
  >> The entire workflow is managed and orchestrated using Airflow, a powerful workflow management tool included in GCP. Airflow ensures the data pipeline runs reliably and efficiently, even at scale.
  >> Cloud Composer, a managed service for Airflow on GCP, is used to simplify deployment and management of Airflow workflows.
  >> Dataproc, a managed Hadoop and Spark service on GCP, is used to create and manage Spark clusters for running PySpark jobs. Airflow dynamically creates and deletes Dataproc clusters as needed to run PySpark jobs, optimizing resource utilization.
  >> Overall, this data processing workflow leverages GCP services to ingest, transform, and store inspection data efficiently, ensuring data availability and facilitating data-driven insights.
