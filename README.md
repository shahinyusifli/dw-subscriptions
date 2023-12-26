# Subscription Medalion Data Warehouse and ELT pipelines within Prefect.

### Architecture
Data applications fetch information through ETL pipelines from the OLTP database. Subsequent transformations occur within the data warehouse, which follows the Medalion Lakehouse Architecture. This architecture facilitates data delivery to various consumers. The warehouse is structured into Bronze, Silver, and Gold stages for efficient data processing. 

The Bronze stage stores raw data using Change Data Capture, preserving historical details for Slowly Changing Dimension Type 1. This stage is useful for consumers such as ML applications and other predictive data applications.

Data undergoes transformation in the Silver Stage, involving type casting and data cleaning. Data historization is ensured through Slowly Changing Dimension Type 2. The structured nature of the data and the absence of Personally Identifiable Information (PII) in dimensions make in-house ELT the chosen method for data processing. Potential consumers of the Silver Stage include Data Marts serving various business objectives and supporting time-series-based analyses.

Transformed data within the data warehouse is modeled into a Kimball-style star schema in the Gold Stage. In this stage, the account dimension and Sales fact table ensure data historization up to Slowly Changing Dimension Type 1. The "Never Process Fact" approach is applied to the transactional sales fact table for Later Arriving Dimensions. This fact table provides detailed information about sales events over time. The Calendar dimension serves as a Role-playing dimension, referenced multiple times by the fact table. The Account dimension can be utilized as a Conformed dimension when needed. Possible consumers of the Gold Stage include quarterly Business Intelligence applications and ad hoc queries.


![alt text](https://github.com/shahinyusifli/dw-subscriptions/document/architecture.png)


### Setup
First of all, all necessary libraries should be installed. For this purpose, you should create a virtual environment and install the needed libraries.

```bash
  $ python -m venv venv
  $ venv\Scripts\activate
  $ pip install -r requirements.txt
```
For setting up source and data warehouse with all needed tables and functions. I'd like to let you know that scripts can be executed. But before you go ahead, please create a Postgres user and change the credentials of inside the provided scripts. 
```bash
  $ python setup_db.py
  $ python setdw_dw.py
```
### Data Pipelines
Prefect is used for monitoring and scheduling pipelines. Delay between pipelines is 15 minutes. For running metioned pipeles, you should login [Prefect Cloud](https://www.prefect.io/cloud) and you should run this script for creating connection:
```bash
  $ cd .\schedular\
  $ prefect cloud login
```
After creating a connection between local and cloud environments. You can use a script or UI for running pipelines. They are scheduled for each day but they can be triggered manually.
You can use these scripts for deploiyng flows to Prefect Cloud. Mentioned stpes should be done for each pipeline/flow. Scripts:
```bash
  $ cd .\scheduler\ 
  $ prefect deployment run '{flow_name/deployment_name}'
  $ python {python file which contains flow}
```

We can see the pipelines below illustration which are scheduled according presented order.

![alt text](https://github.com/shahinyusifli/dw-subscriptions/document/prefect-dasboard.png)

#### Data Extraction
Data extraction takes snapshots of data daily. The daily snapshot contains inserted and updated data. 

#### Data Loading
The data that has been extracted is sent to the Bronze stage of the data warehouse using Change Data Capture. 

#### Data Transformation
In the Silver stage of the data warehouse, all data transformation processes take place. PL/pgSQL is employed for processing, enhancing the speed and performance of transformations, particularly in environments where storage and processing are distinct.

Transformations in this context involve type casting, which converts character-based date descriptive information into date types. This process is crucial as it enables us to conduct historical data analyses based on quarters, holidays, or other necessary date details. To facilitate casting data, the calendar table supplies a two-year range of data. Additionally, the plan duration attribute is reformatted from a categorical data format to a numerical one.

During the Silver Stage, data is flagged, and only valid data is structured into a Kimball-style star schema. The current valid data is then modeled into one transactional fact table and four dimensions.

Listed pipelines are used for this purpose:
- to_oltp_database_flow
- to_bronze_flow
- to_dim_account_flow
- to_dim_calendar_flow
- to_dim_device_flow
- to_dim_subscription
- to_fct_sales_flow

## Data Glossary

| Field Name                | Description                                                                                           | Type                       |
|---------------------------|-------------------------------------------------------------------------------------------------------|----------------------------|
| id                        | Unique identifier for each record.                                                                    | Unique Identifier          |
| subscription_type         | Type of subscription (e.g., 'Basic', 'Premium', 'Standard').                                           | Categorical                |
| monthly_revenue           | Monthly revenue associated with different subscription plans.                                           | Numerical List             |
| join_date_range           | Range of dates during which users joined the subscription service.                                      | Date Range                 |
| last_payment_date_range   | Range of dates indicating the period of the last payment made by users.                                  | Date Range                 |
| cancel_date_range         | Range of dates indicating the period during which users canceled their subscription (nullable).       | Nullable Date Range        |
| country                   | Country of residence for the user (e.g., 'Brazil', 'Italy', 'UK', 'US', 'Germany', 'Mexico', 'France'). | Categorical                |
| age_range                 | Minimum and maximum age allowed for users.                                                            | Tuple (Minimum, Maximum)   |
| gender                    | Gender of the user (e.g., 'Male', 'Female').                                                          | Categorical                |
| device                    | Device used by the user for accessing the subscription service.                                        | Categorical                |
| plan_duration             | Duration of the subscription plans (e.g., '1 month', '6 month').                                       | Categorical                |
| active_profiles           | Number of active profiles associated with a user account.                                             | Numerical List             |
| household_profile_ind     | Binary indicator (1 or 2) representing whether the user has multiple profiles within the household.  | Binary                     |
| movies_watched            | Count of movies watched by the user.                                                                 | Numerical List             |
| series_watched            | Count of series watched by the user.                                                                 | Numerical List             |


