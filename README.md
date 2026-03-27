
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
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/TaskScheduledInAirFlow.png)

Airflow Orchestration
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/AirflowOrchestration.png)

Extracted JSON Data Stored in Data Lake (GCS)
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/JSONStoredInGCS.png)

Airflow Task Submit Dataflow batch processing Job
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/Dataflow%20Job.png)

Data Storage in BigQuery
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/BigQuery%20Tables.png)

dbt Cloud Data Modeling
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/lineage%20diagram.png)

dbt job Scheduled
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/dbt%20job%20scheduled.png)

dbt job run
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/dbt%20job.png)

BigQuery Report Tables
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/rpt_ny_traffic_event_county.png)

Dashboard 1
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/Dashboard%201.png)

Dashboard 2
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/DashBoard%202.png)

Dashboard 3
![App Screenshot](https://github.com/anilbhaila/de-zc-finalproject/blob/main/images/Dashboard%203.png)
## Data Source 
New York Traffic Events
API Documentation Link: https://511ny.org/help/endpoint/event


## Steps to Run End to End Data Pipeline

Prerequisites
1. Local Machine or Google Virtual Machines 8GB+ RAM 20+ GB Storage
2. Git Installed, uv installed
3. Terraform Installed
4. Google Cloud Platform Account
5. Docker and Docker Compose Installed
6. dbt Cloud Account
7. 511ny.org Account to request API Key


Steps:
1. Project Preparation
    Clone the Git Repository
    > git clone git@github.com:anilbhaila/de-zc-finalproject.git
    > cd de-zc-finalproject
    > ls
    README.md  data  dbt  extraction  images  orchestration  pyproject.toml  terraform  uv.lock

    > uv sync

    Set Up Environment Variables
    Create a .env file in root directory. Add below code:
    511NY_API_KEY=your-api-key

    Note: this .env is only used in test script inside extraction folder.

2. Google Cloud Platform (GCP) Setup

    Create your GCP Account using any gmail account.
    > Go to Google Cloud Console
    > Create a new project and note the project ID and project number.
    > Enable billing (requires a credit card, $300 free credits is provided for 90 days)

    Create Service Account and Authorization
    > Go to IAM & Admin > Service Accounts
    > Create a new service account
    > Grant the service account the following permissions:
        1. Viewer
        2. Storage Admin
        3. Storage Object Admin
        4. BigQuery Admin
        5. Dataflow Admin
        6. Compute Admin
    
    > Create and download the JSON format key file.
        By default "Disable service account key creation" policy is activated in all project. You need to disable it and then create and download the Service Account key.

        You can disable this policy from Organizational policies.
        Select Your Project > Search Organizational Policies in Search bar > Click "Disable service account key creation" > Manage policy > Policy source > Override parent's policy > Add Rule > Enforcement > off

        Now refresh your browser
        Select your project > Search "Service Accounts" in search bar > select your newly created Service Account > Keys > Add key > Create new key > JSON
        Your key file will be downloaded. Keep this key safe.

        Enable Required APIs
        > Compute Engine API
        > BigQuery API
        > Dataflow API
        > Cloud Storage API

3. Create GCP Infrastructure with Terraform
    > Navigate to the terraform directory:
    > cd terraform
    > mkdir .keys
    > ls
    main.tf                         outputs.tf                      terraform.tfstate               terraform.tfstate.backup        variables.tf  .keys

    Add your service account JSON key file in this .keys directory and rename it as "gcp-creds.json"

    Now edit variables.tf file as per your needs
    e.g 
    variable "project" {
        description = "Your GCP project ID"
        default = "your-project-id"
    }

    After modifying all variables, run below command:
    > terraform init
    > terraform plan
    > terraform apply

    If no errors, all GCP resources should have been created. Check GCS bucket, BigQuery datasets

    > terraform destroy (to remove your GCP resource)

4. Set Up Airflow Workflow orchestration
    Navigate to orchestraton directory
    > cd ../orchestration
    > ls 
    Dockerfile              dags                    docker-compose.yaml     requirements.txt

    Run below command, to run docker containers
    > docker-compose up -d

    If you are using VS Code, you might need to port forward 8085 in PORTS tab, it it is not auto forwarded.

    Then, you will be able to access Airflow Web UI at http://localhost:8085

    Login with the default username and password (airflow/airflow)
    You can change these password in docker-compose.yaml file.

    # Configure Airflow Connections
        1. Go to Admin > Connections > click +
            > Connection Id: google_cloud_default
            > Connection Type: Google Cloud
            > Project Id: Your Project Id
            > Keyfile JSON: Copy your Service Account JSON file's content and paste here
            > Save

        2. Go to Admin > Variables > click + 
            > Key: ny_traffic_api_key Val: Your API Key
            > Key: project_id Val: Your GCP Project Id
            > Key: svc_AccountEmail Val: Your Service Account Email
            > Key: location Val: Your Project Location e.g. us-south1
            > Key: task_schedule_interval Val: 15

            These key's are case-sensitive

    > Now Check on DAGs tab, you should see "ny_traffic_events_pipeline" listed. 

    > Enable this DAG. By default, this task is scheduled to run every 10 minutes if "task_schedule_interval" variable is not defined. You can update this interval by updateing variable "task_schedule_interval"

    > You will see the scheduled task run successfully.

    > Check if the submitted Dataflow Job ran successfully in GCP: Search Dataflow in Search bar. you will see all submitted jobs. After successfully processed job, JSON data will be stored into BigQuery dataset named "ny_traffic_events_raw.ny_traffic_events"

6. Set Up dbt Cloud for Data Transformation:
    # Register and Configure dbt Cloud
    > Create dbt Cloud acount, go to https://cloud.getdbt.com
    > Create a new Project
        > Click "Start a New Project"
        > Project name: "Give your project name"
        > Connection: Add New Connection
            > Click BigQuery
                > Connection name: BigQuery
                > Select Adapter: BigQuery
                > Settings: Service Account JSON > Click Upload a Service Account JSON file
                > Upload your Service Account JSON File you created above
                > Test Connection
        > Development credentials
            > Autthentication Method: Service Account JSON
            > Dataset: dbt_dev_ny_traffic_events
            > Target name: deault

    # Connect dbt Repository
    > Configure GitHub connection in dbt Cloud:
        > Select "Link an Existing GitHub Repository"
        > Select your project
        > You will see "Your project is ready!'
        > Start developing in the Studio

    > When you go to Studion, you will see "Initialize dbt project", this happens because our dbt project is not in our root directory. It's in dbt directory. So, we need to change the directory.

    # Change the dbt project directory
    > Click you profile > Your Profile > Projects > ny_traffic_events > Edit > project subdirectory > dbt > save

    > Go back to studio, you will be asked to restart studio, click ok
    > now you will not be asked to initialize dbt project.

    > In commands run below command:
    > dbt run

    All 6 models should pass with 0 errors.
    
    Check your BigQuery, below dataset should be created:
    > dbt_dev_ny_traffic_events
        > dim_traffic_events
        > fact_ny_traffic_events
        > rpt_ny_traffic_event_county
        > rpt_ny_traffic_event_live_data
        > rpt_ny_traffic_event_type
        > stg_ny_traffic_events


    If you want to make modifications you can. to push your modified code you need to set up SSH deploy key.
    > How to get SSH deploy key in dbt cloud?
    > Go to your Projects > click ny_traffic_events > click Repository 
    > Repository Details page will open
        > Copy this Deploy Key

    > Go to your Git Hub Repository, select your project repo and click Project Settings
        > Deploy Keys > Add deploy key > paste your dbt Deploy key
        > Make sure to check the "Allow write access"

7. Scheudle dbt orchestration
    Go to Orchestration
    > Environment settings
        > Click + Create Environment button
            > Environment name: PROD 
            > Environment type: Deployment
            > Set deployment type: PROD Production
            > dbt version: Latest
       
    > Connection profiles
        > Click + Assign profile
            > Click + Create profile
                > Profile name: PROD
                > Connection details: BigQuery
                > Deployment credentials:
                    > Autthentication Method> Service Account JSON
                    > Dataset: dbt_prod_ny_traffic_events

                    >Click Test connection
                    > Create profile

    > Assign connection profile: PROD   
    > click + Assign profile
    > Save

    > Orchestration > Jobs > Click + Create job > Deploy job
        > Job name: ny_traffic_events_hourly_run   
        > Environment: New Environment PROD
        > commands
            > Check Run source freshness
            > dbt run
        > Run on schedule
            > Every 1 Hour

    > Save

    Now every hour the dbt models will run and update our rpt tables with new data.

8. Create Looker Studio Dashboard.
    > Go to Looker Studio
    > Click File > New Report > Add data to report > BigQuery > My Project > "Select Your Project" > dbt_prod_ny_traffic_events > rpt_ny_traffic_event_county > Add

    
    # Dashboard 1: New York Traffic Events
        > Click Add a chart 
        > Drop and Drag Vertival Bar Chart
            Bar chart properties
            > Data source: rpt_ny_traffic_event_county

            Setup
            > Dimension - X axis > rpt_ny_traffic_event_type
            > Metirc -Y axis > Record rpt_ny_traffic_event_county

        
        > Click Add a Chart > Pie Chart
            Bar chart properties
            > Data source: rpt_ny_traffic_event_county

            Setup
            > Dimension: County
            > Metric: Record Count

    # Dashboard 2: Events by Type and by County
        > Click Add a chart > Scorecard
            > Data source: rpt_ny_traffic_event_county

            Setup
            > Primary field > Metric > Record Count
                > Change Display Name of Record Count > Total Events

        > Click Add a chart > Scorecard
            > Data source: rpt_ny_traffic_event_county

            Setup
            > Primary field > Metric> Drop and Drag DataRefreshedDate from Data panel.
               > Change Aggrigation function to MIN


        > Click Add a chart > Pivot tables
            > Data source: rpt_ny_traffic_event_county

            Setup
            > Row Dimension: County
            > Column Dimension: EventType
            > Metric: SUM(data_points)


        > Click Add a chart > Treemap
            > Data source: rpt_ny_traffic_event_county

            Setup
            > Dimension: EventType
            > Metric: SUM(data_points)

    # Dashboard 3: New York Traffic Events Real Time Map

        > Click Add a control > Drop-down list
            > Data source: rpt_ny_traffic_event_live_data

            Setup
            > control field: EventType

            > Metric: SUM(data_points)
 
        > Click Add a chart > Scorecard
            > Data source: rpt_ny_traffic_event_live_data

            Setup
            > Primary field > Metric > Record Count
                > Change Display Name of Record Count > Total Events

        > Click Add a chart > Scorecard
            > Data source: rpt_ny_traffic_event_county

            Setup
            > Primary field > Metric> Drop and Drag DataRefreshedDate from Data panel.
               > Change Aggrigation function to MIN

        > Click Add a chart > Google Maps
            > Data source: rpt_ny_traffic_event_live_data

            Setup
            > Fields > Location: GeoLocation
            > Tooltip: RoadwayName
            > Color Dimension: EventType

            Note: GeoLocation is calculated filed created in Looker Studio by Concatinating two fields Latitude and Longitude.

        > How to create calculated filed?
            > click + Add a field in Data panel.
                > Add calculated field
                > Name: GeoLocation
                > Formula: CONCAT(Latitude,",",Longitude)
                > Change Data type > Geo > Latitude, Longitude

## Live Report
<iframe width="600" height="450" src="https://lookerstudio.google.com/embed/reporting/5d3af8c9-735e-40e6-9bbd-e2f4703c7b2d/page/joGtF" frameborder="0" style="border:0" allowfullscreen sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"></iframe>