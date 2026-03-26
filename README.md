
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





## Documentation

[Documentation](https://linktodocumentation)



## Screenshots

![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/ProjectArchitecture.png)


