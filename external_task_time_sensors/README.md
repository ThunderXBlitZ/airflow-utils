
### External Task Time Sensors

Didn't see such functionality in Airflow's provided [Sensor classes](https://github.com/apache/airflow/tree/main/airflow/sensors). So I built my own.

Provide either sensor classes with an external dag name (and optional task id), and a timedelta or timezone, and you can start poking for external DAGs of any run time! No need to be coupled to the crontab.

Might not be so relevant with the Producer-Consumer model in Airflow 2.6, but this may benefit people still on older Airflow versions.

<br>

*Pro-tip: Check your Airflow DB has indexed the required columns.
Note: Have not tried in a production setup.*

<br>

Usage:
```
from datetime import timedelta, timezone
from airflow import DAG
from airflow.utils.dates import days_ago

from plugins.operators.custom_sensor import TimeRangeExternalTaskSensor, \
    SameDayExternalTaskSensor

default_args = {
    "owner": "hello_world",
    "start_date": days_ago(1)
}

with DAG("test_dag", default_args=default_args, schedule_interval="0 0 * * *", catchup=False) as dag:

    sensor_timedelta = timedelta(hours=6)
    dag_timezone = timezone.utc

    # pokes for any dag run within the current dag's run time and the timedelta provided
    sensor_a = TimeRangeExternalTaskSensor(task_id='sensor_a',
                                           external_dag_id='external_dag_id',
                                           execution_delta=sensor_timedelta,
                                           external_task_id='external_task_id',
                                           timeout=7200 # 2 hours
                                           )

    # pokes for any dag run within the same day as current dag's run time
    sensor_b = SameDayExternalTaskSensor(task_id='sensor_b',
                                         external_dag_id='external_dag_id',
                                         external_task_id='external_task_id',
                                         dag_timezone=dag_timezone,
                                         timeout=7200 # 2 hours
                                         )

    sensor_a >> sensor_b
```