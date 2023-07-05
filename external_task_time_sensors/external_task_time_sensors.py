from typing import Optional
from datetime import datetime, timedelta, timezone
from functools import partial

from sqlalchemy import and_
from airflow import settings
from airflow.models import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.external_task import ExternalTaskSensor


class TimeRangeExternalTaskSensor(ExternalTaskSensor):
    # sensor class that scans for any sucessful dag run of the target dag, in the time range (inclusive):
    # sensor's execution_date - execution_delta, sensor's execution_date

    def __init__(self,
                 external_dag_id: str,
                 execution_delta: timedelta,
                 external_task_id: Optional[str] = None,
                 *args, **kwargs):

        super().__init__(
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            execution_delta=None,
            *args, **kwargs)

        self.execution_date_fn = partial(_check_dag_run_within_range,
                                         execution_delta=execution_delta,
                                         external_dag_id=external_dag_id,
                                         external_task_id=external_task_id)


class SameDayExternalTaskSensor(ExternalTaskSensor):
    # sensor class that scans for any sucessful dag run of the target dag,
    # that is within the same day

    def __init__(self,
                 external_dag_id: str,
                 external_task_id: Optional[str] = None,
                 dag_timezone: Optional[timezone] = None,
                 *args, **kwargs):
        # dag_timezone: both current and target dags's execution_date will be adjusted to this timezone
        # Airflow BE uses UTC by default

        super().__init__(
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            execution_delta=None,
            *args, **kwargs)

        self.execution_delta = None
        self.execution_date_fn = partial(_check_dag_run_same_date_and_earlier,
                                         external_dag_id=external_dag_id,
                                         external_task_id=external_task_id,
                                         dag_timezone=dag_timezone)


def _check_dag_run_within_range(dt:datetime,
                                execution_delta:timedelta,
                                external_dag_id:str,
                                external_task_id:str=None,
                                **kwargs) -> datetime:
    # get execution_date of latest dag run of target dag
    # if execution_date is within range, then return the execution_date
    # else return a nonsensical date for Airflow, for it to fail properly

    start_dt, end_dt = dt - execution_delta, dt
    target_obj = None
    if external_task_id is None:
        target_obj = _check_exists_dag_run_execution(external_dag_id, start_dt, end_dt)
    else:
        target_obj = _check_exists_task_instance_execution(external_dag_id,
                                                             external_task_id,
                                                             start_dt,
                                                             end_dt)
    return target_obj.execution_date if target_obj else datetime(1970, 1, 1, tzinfo=timezone.utc) # nonsensical date for Airflow

def _check_dag_run_same_date_and_earlier(dt:datetime,
                                         external_dag_id:str,
                                         external_task_id:str=None,
                                         dag_timezone:timezone=timezone.utc,
                                         **kwargs) -> datetime:
    # get execution_date of latest dag run of target dag.
    # if execution_date is same day as the reference_dt, and earlier,return the execution_date
    # else return a nonsensical date for Airflow, for it to fail properly.

    dt = dt.astimezone(tz=dag_timezone)
    start_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    execution_delta =  dt - start_dt
    execution_delta = timedelta(seconds=execution_delta.seconds)
    output_dt = _check_dag_run_within_range(dt, execution_delta, external_dag_id, external_task_id)
    output_dt = output_dt.astimezone(tz=timezone.utc)
    return output_dt

def _check_exists_dag_run_execution(external_dag_id: str,
                                    start_dt: datetime,
                                    end_dt: datetime,
                                    ) -> Optional[DagRun]:
    # Helper func: check that a dag run for the target dag_id exists within the time range given
    # Returns the DagRun if exists

    dag_runs = DagRun.find(dag_id=external_dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    for _run in dag_runs:
        if _run.execution_date < start_dt:
            return None
        elif start_dt <= _run.execution_date <= end_dt:
            return _run

def _check_exists_task_instance_execution(dag_id: str,
                                    task_id: str,
                                    start_dt: datetime,
                                    end_dt: datetime,
                                    ) -> Optional[TaskInstance]:
    # Helper func: check that a task instance for the dag_id exists within the time range given
    # Returns the TaskInstance if exists

    session = settings.Session()
    task_instances = session.query(TaskInstance).filter(
        and_(
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id == task_id
        )
    ).all()

    task_instances.sort(key=lambda x: x.execution_date, reverse=True)
    for _ti in task_instances:
        if _ti.execution_date < start_dt:
            return None
        elif start_dt <= _ti.execution_date <= end_dt:
            return _ti
