from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2005, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")

run_first = DummyOperator(
    task_id='run_first',
    dag=dag
)

run_join = DummyOperator(
    task_id='run_join',
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

rocket_list = ["all", "falcon1", "falcon9", "falconheavy"]

for rocket in rocket_list:
    t1 = BashOperator(
        task_id="get_data_" + rocket, 
        bash_command="python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }} -o /var/data {{ params.rocket }}",
        params={"rocket": " -r " + rocket if rocket != "all" else ""},
        dag=dag
    )

    t2 = BashOperator(
        task_id="print_data_" + rocket, 
        bash_command="cat /var/data/year={{ execution_date.year }}/rocket={{ params.rocket }}/data.csv", 
        params={"rocket": rocket},
        dag=dag
    )

    run_first >> t1 >> t2 >> run_join
