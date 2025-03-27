from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from cosmos import DbtTaskGroup
from datetime import datetime

from utils.csv_load_utils import upload
from utils.config import DBTConfig, DataSet
from utils.on_failure_callbacks import failure_callbacks

# Define DAG
with DAG(
    dag_id="test_update_dag",
    start_date=datetime(2024, 7, 4),
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
    render_template_as_native_obj=True,
    default_args={
        "on_failure_callback": failure_callbacks,
        "trigger_rule": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    },
) as test_update_dag:

    datasets = [
        DataSet.CENTRAL_MAPPING,
        DataSet.DEALS,
        DataSet.MANUAL,
        DataSet.ROUTY,
        DataSet.SCRAPERS,
        DataSet.VOLUUM_MAPPER,
        DataSet.VOLUUM,
    ]

    def get_task_ids_for_run(**context):
        """Determines which dbt models need to be executed."""
        task_instance = context['ti']
        tasks = []
        for dataset in datasets:
            task_id = f'raw_data_load.upload_{dataset.value}'
            table_name = task_instance.xcom_pull(task_id)
            if table_name:
                task_id = f'base_{table_name}_run'
            if task_id in task_instance.task.downstream_task_ids:
                tasks.append(task_id)

        print(f'Tasks to be updated: {tasks}')
        return tasks

    # Define tasks
    first_task = EmptyOperator(task_id="first_task")
    last_task = EmptyOperator(task_id="last_task")

    with TaskGroup(group_id='raw_data_load') as raw_data_load:
        for dataset in datasets:
            PythonOperator(
                task_id=f'upload_{dataset.value}',
                python_callable=upload,
                op_kwargs={'dataset': dataset}
            )

    run_dbt_technical_models = DbtTaskGroup(
        group_id="run_dbt_technical_models",
        render_config=DBTConfig.render_config(
            select=["path:models/bronze/pipeline__tech"]
        ),
        project_config=DBTConfig.project_config(),
        execution_config=DBTConfig.execution_config(),
        profile_config=DBTConfig.profile_config(),
        default_args={"retries": 1, "install_deps": True}
    )

    select_general_dbt_models = BranchPythonOperator(
        task_id="select_general_models",
        python_callable=get_task_ids_for_run,
        provide_context=True
    )

    dbt_pipelines = []
    for i in range(4):
        dbt_pipelines.append(DbtTaskGroup(
            prefix_group_id=False,
            group_id=f"run_dbt_pipeline_{i}",
            render_config=DBTConfig.render_config(
                select=[f"path:models/bronze/pipeline_{i}"]
            ),
            project_config=DBTConfig.project_config(),
            execution_config=DBTConfig.execution_config(),
            profile_config=DBTConfig.profile_config(),
            default_args={"retries": 1, "install_deps": True}
        ))

    select_dbt_models = BranchPythonOperator(
        task_id="select_dbt_models",
        python_callable=get_task_ids_for_run,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    # Define task dependencies
    (
        first_task >> raw_data_load >> run_dbt_technical_models >>
        select_general_dbt_models >> dbt_pipelines[0] >> select_dbt_models
        >> [dbt_pipelines[1], dbt_pipelines[2]] >> dbt_pipelines[3]
        >> last_task
    )
