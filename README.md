# Data Modeling of New York City Food Inspection Dataset on Google Cloud Using PySpark and Airflow

This project outlines the data processing workflow for ingesting and transforming data from an API, performing inspections, and storing the results in Parquet tables. The entire process is orchestrated and managed through Airflow (Cloud Composer) on Google Cloud Platform (GCP).

Check the Final Dashboard: [PowerBi Dashboard](https://shorturl.at/fpMQ0)

## Data Flow:

### Data Acquisition:
- Data is retrieved from an external API.
- Python and PySpark are used to process and prepare the data for further processing.

### Raw Data Storage:
- The processed data is temporarily saved in a Google Cloud Storage (GCS) bucket.
- GCS provides a scalable and cost-effective storage solution for large datasets.

### Data Processing:
- Data is read from the GCS bucket.

#### Dimension Tables:
- Data is used to create dimension tables, which provide context and meaning to fact tables. These tables store descriptive attributes about entities in your data model.

#### Fact Tables:
- **Food Inspection:** The data is processed to create fact food inspection tables. These tables store detailed information about food inspections, such as inspection dates, results, and violations.
- **Violation:** Data is processed to create a fact violation table. These tables store specific violations found during inspections, along with details like violation types and severity levels.

#### Dimensional Model:
![Uploading NYC_Food_inspections_DimensionalModel.jpg…]()


### Data Storage:
- The processed data is stored in Parquet format, a columnar data format optimized for fast querying and efficient data compression. Parquet is well-suited for data warehouses and analytics due to its performance benefits.

### Orchestration and Management:
- The entire workflow is managed and orchestrated using Airflow, a powerful workflow management tool included in GCP. Airflow ensures the data pipeline runs reliably and efficiently, even at scale.
- Cloud Composer, a managed service for Airflow on GCP, is used to simplify deployment and management of Airflow workflows.
- Dataproc, a managed Hadoop and Spark service on GCP, is used to create and manage Spark clusters for running PySpark jobs. Airflow dynamically creates and deletes Dataproc clusters as needed to run PySpark jobs, optimizing resource utilization.
- Overall, this data processing workflow leverages GCP services to ingest, transform, and store inspection data efficiently, ensuring data availability and facilitating data-driven insights.

  
## Airflow Workflow:
![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/80d9da14-059e-4f05-b5ec-960b057b903e)

![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/6cd42b1e-d7ab-4e8e-b3c9-7dbdff10b99a)

## Dataproc Cluster:
![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/768f67a9-c8d4-4c86-9acc-9d6f2b8f0ae3)

![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/5c029c7d-a5f2-4b8d-80d9-e6b8a0d1dc4a)

## GCS Bucket:
![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/6e2029ef-3a1b-4d03-be49-9e0651ac0211)

![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/6d90ae6f-072a-42ce-8452-9214b0783b1a)

## Pyspark Jobs running on Cluster:
![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/36042146-8948-4a40-847b-db8b99f134de)

![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/539d2172-9469-416b-8586-f35682e64fef)

![image](https://github.com/girish-Pillai/NYC_Food_Inspection_Data_Modeling/assets/98634040/65312ad4-9e6f-4230-9180-05cd99f9057f)


