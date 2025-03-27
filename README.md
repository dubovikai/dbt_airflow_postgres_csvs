# Data Pipeline with Airflow, dbt, and PostgreSQL

## Overview
The goal of this project was to develop a data pipeline using Airflow, dbt, and PostgreSQL.

Successfully integrated Airflow v2.10.2 and DBT-Core v2.9.2 using Astronomer Cosmos.

## Running the Project
To start the project locally, execute the following command in the root directory:

```sh
docker compose up
```
```
Airflow UI:
http://localhost:8080
log/pass: airflow

BD: 
localhost:5433
user: postgres
pass: simple_password
db_name: test_dwh_db
schema: bronze
```

## Simulation of Sequential Updates Over Several Days with Data Correction

The simulation of sequential updates over several days with data correction can be implemented as follows:

1. Execute `source day1.sh`
2. Run the DAG
3. Execute `source day2.sh`
4. Run the DAG

DBT model development can be conducted using the console.

## DAG Structure
The "test_update_dag" DAG consists of ten high-level tasks (in order of execution):

![Here should be an image](https://github.com/dubovikai/dbt_airflow_postgres_csvs/blob/master/dag.png?raw=true)

### first_task
- `EmptyOperator` as the first DAG step.

### raw_data_load
Loads data from the CSV storage. Locally, the storage is the `external_file_storage` directory.

- Files located in the root of this directory fully overwrite the corresponding table.
- Files located in subdirectories are incrementally inserted into the tables.
- If a file with the same name as a previously processed file is encountered, the data loaded from the previous file is entirely replaced with the new data.
- After processing, the file is renamed by adding the prefix `.pX.`, where `X` is the file ID in the log table.

(See `dbt/models/bronze/pipeline__tech/latest_loaded_files.sql` for more details.)

### run_dbt_technical_models
- Executes models related to the file log.

- In the current implementation, each DAG run fully recalculates the data in dependent tables.
- Incremental updates of all tables are technically possible but not yet implemented.

### select_general_models
- selector for dbt models necessary to update in the `run_dbt_pipeline_0` task group.
#### run_dbt_pipeline_0
- dbt models used in the first and second pipelines.

### select_dbt_models
- selector for dbt models necessary to update in the `run_dbt_pipeline_1` and `run_dbt_pipeline_2` pipelines.
#### `run_dbt_pipeline_1` 
- dbt models of the first pipeline.
#### `run_dbt_pipeline_2` 
- dbt models of the second pipeline (monthly earnings for operators are not implemented due to insufficient data).

### run_dbt_pipeline_3
- dbt models of the third pipeline.

### last_task
- `EmptyOperator` as the final DAG step.

As part of the test assignment, data tests have not been implemented since the nature of the data is unclear, and there is no understanding of what entities these data describe.  

In particular, it is unclear which fields in the source data can be considered primary keys and what value ranges can be considered reasonable.

The DAG structure implements the technical requirements, and task groups define the boundaries of the pipelines (Pipeline 1, Pipeline 2, Pipeline 3).

A more flexible approach seems to be avoiding a large number of small pipelines and processing all DBT models within a single group. With this approach, there is no need to track table dependencies manually, as the entire orchestration will be handled automatically by DBT. This eliminates the need for a separate pipeline_0, and if dependencies arise between pipeline_1 and pipeline_2, it will be less painful to manage. (See GitHub branch ["simplified"](https://github.com/dubovikai/dbt_airflow_postgres_csvs/blob/simplified/airflow/dags/test_update_dag.py))


## Out of Scope
The following aspects were not considered within the scope of this task:
- Data access control and permissions.
- Dedicated storage for sensitive information (e.g., GDPR, financial data).
- Secret management (e.g., storing credentials in a secure backend).
- Fine-tuning Airflow and PostgreSQL (used with default configurations).
- Deployment process in a production environment. The manifest file is currently under Git, but it is preferable to parse the project during deployment.

**P.S.: Slack notifications for task failures have been implemented but are currently disabled.**
