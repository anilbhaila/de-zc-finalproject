
# New York Traffic Events Visualization Project

This project is trying to Visualize the New York Traffic Events in Real Time. For the past few weeks we are learning data engineernig with Data Talks Club, Data Engineering Zoom Camp Course. I have tried to implement the knowledge we learned in this course.




## Overview
This project is a complete end-to-end data engineering pipeline that extracts, processes, and analyzes real-time New York Traffic Events data from the 511ny.org API.

Terraform, Docker, Airflow, Google Cloud Storage (Bucket), BigQuery, dbt (data build tool), Lookerstudio are used to build this project.


## Tech Stack

**Containerization Platform (Docker):**
To run our python script that extracts data from 511ny.org api, we need a Workflow Orchestration Software, Airflow. Docker is used to run our Airflow.

**Data Lake (GCS Bucket)**
To store the extracted JSON data from 511ny.org API, we need a data lake. So, Google Cloud Storage (GCS) bucket is used to store all extracted New York Traffic Events data in JSON file.

**Data Warehouse (BigQuery)**
After we extracted and store New York Traffic Events data in GCS, we need to process them to derive meanings. Thus we used BigQuery to store data so that we can query them in milliseconds.

**Infrastracture as Code (Terraform)**
As we used GCP resources for our project, it became tidious job to create resources and remove this resources from clicking here and there in UI console. All these cloud resources are expensive as well. So, as soon as we don't need them, we need to release these GCP resources by one command. Thus we need to use Terraform. It is so easy to create and remove GCP resources.

**Workflow Orchestration(Airflow)**
Airflow supports python scripts to define Airflow Tasks and we can update our python scripts in real time without needing to restart Airflow. Thus i liked Airflow better than Kestra. Orchestration tools help us to schedule task such as extraction, processing, submitting task to GCP resources like Dataflow Jobs.

**Data Transformation (dbt Cloud)**
dbt Cloud is so easy to use and very powerful in Transformation of data. We can manage our dbt code in any Version Control System. dbt's modular desing and testing framework is very robust which ensures the data quality and consistency.

**Data Visualization (Looker Studio)**
Looker Studio is used to visualize our New York Traffic Events data in real time. We can connect BigQuery directly into Looker Studio.





## Project Architecture
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/ProjectArchitecture.png)

Data Flow Explanation:
1. Data Extraction: Our Python API Client executed inside Airflow task regurly fetches the latest NY Traffic Events Data from 511ny.org API. format.

2. Data Storage: The Data Extracted from the API are stored into GCS Bucket (Data Lake) in a JSON file with a scheduled task.

3. Batch Processing: Airflow Task also submit Batch Processing task to Google Dataflow Job. This Batch Processing Job will process JSON Files from Data Lake (GCS) and do some initial transformation then the structured data is stored into Dataware House (BigQuery)

4. Data Modeling: Our dbt Cloud Scheduled Job performs further data transformation and Modeling in BigQuery and store it back to BigQuery dataset to be used for data visualization

5. Data visualization: We use the data transformed by dbt Cloud in to Looker Studio for data visualization.
## Snapshots
Data Extraction Task Scheduled in Airflow
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/TaskScheduledInAirFlow.png)

Airflow Orchestration
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/AirflowOrchestration.png)

Extracted JSON Data Stored in Data Lake (GCS)
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/JSONStoredInGCS.png)

Airflow Task Submit Dataflow batch processing Job
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/Dataflow%20Job.png)

Data Storage in BigQuery
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/BigQuery%20Tables.png)

dbt Cloud Data Modeling
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/lineage%20diagram.png)

dbt job Scheduled
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/dbt%20job%20scheduled.png)

dbt job run
![App Screenshot]
https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/dbt%20job.png

BigQuery Report Tables
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/rpt_ny_traffic_event_county.png)

Dashboard 1
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/Dashboard%201.png)

Dashboard 2
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/DashBoard%202.png)

Dashboard 3
![App Screenshot]
(https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/Dashboard%203.png)