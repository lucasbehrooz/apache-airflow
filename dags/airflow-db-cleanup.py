"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid
having too much data in your Airflow MetaStore.

airflow trigger_dag --conf '{"maxDBEntryAgeInDays":30}' airflow-db-cleanup

--conf options:
    maxDBEntryAgeInDays:<INT> - Optional
"""

import airflow
from airflow import settings
from airflow.configuration import conf
from airflow.models import DAG, DagTag, DagModel, DagRun, Log, XCom, SlaMiss, TaskInstance, Variable
from airflow.jobs.base_job import BaseJob
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import dateutil.parser
import logging
import os
from sqlalchemy import func, and_
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import load_only

try:
    # airflow.utils.timezone is available from v1.10 onwards
    from airflow.utils import timezone
    now = timezone.utcnow
except ImportError:
    now = datetime.utcnow

# airflow-db-cleanup
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
START_DATE = airflow.utils.dates.days_ago(1)
# How often to Run. @daily - Once a day at Midnight (UTC)
SCHEDULE_INTERVAL = "@daily"
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = []
# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove those files that are 30 days old or older.

DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = int(
    Variable.get("airflow_db_cleanup__max_db_entry_age_in_days", 30)
)
# Prints the database entries which will be getting deleted; set to False to avoid printing large lists and slowdown process
PRINT_DELETES = True
# Whether the job should delete the db entries or not. Included if you want to
# temporarily avoid deleting the db entries.
ENABLE_DELETE = True

# get dag model last schedule run
try:
    dag_model_last_scheduler_run = DagModel.last_scheduler_run
except AttributeError:
    dag_model_last_scheduler_run = DagModel.last_parsed_time

# List of all the objects that will be deleted. Comment out the DB objects you
# want to skip.
DATABASE_OBJECTS = [
    {
        "airflow_db_model": BaseJob,
        "age_check_column": BaseJob.latest_heartbeat,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date,
        "keep_last": True,
        "keep_last_filters": [DagRun.external_trigger.is_(False)],
        "keep_last_group_by": DagRun.dag_id
    },
    {
        "airflow_db_model": TaskInstance,
        "age_check_column": TaskInstance.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": Log,
        "age_check_column": Log.dttm,
        "keep_last": True,
        "keep_last_filters": [Log.dag_id != DAG_ID],
        "keep_last_group_by": Log.dag_id
    },
    {
        "airflow_db_model": XCom,
        "age_check_column": XCom.execution_date,
        "keep_last": True,
        "keep_last_filters": [XCom.dag_id != DAG_ID],
        "keep_last_group_by": XCom.dag_id
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column": SlaMiss.execution_date,
        "keep_last": True,
        "keep_last_filters": [SlaMiss.dag_id != DAG_ID],
        "keep_last_group_by": SlaMiss.dag_id
    }
]

args = {
    'owner': DAG_OWNER_NAME,
    'start_date': START_DATE,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(DAG_ID, schedule_interval=SCHEDULE_INTERVAL, default_args=args)


def print_configuration_func(**kwargs):
    logging.info("-------------CONFIGURATION-------------")
    logging.info(
        "{:<30} | {:<30}".format("Conf Option", "Value")
    )
    logging.info(
        "{:<30} | {:<30}".format("maxDBEntryAgeInDays", str(DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS))
    )
    logging.info("----------------------------------------")
    return True


print_configuration = PythonOperator(
    task_id='print_configuration',
    python_callable=print_configuration_func,
    provide_context=True,
    dag=dag,
)


def cleanup_callable(session, age_in_days, print_deletes, enable_delete, airflow_db_model, age_check_column, keep_last=False, keep_last_filters=None, keep_last_group_by=None, func_filter_by_column=None):
    """
    Cleanup db entries
    :param session: DB Session
    :param age_in_days: Entry Age in days
    :param print_deletes: Print deletes flag
    :param enable_delete: Enable delete flag
    :param airflow_db_model: DB Model to delete
    :param age_check_column: Column to use to calculate age
    :param keep_last: Keep last n records; Default: False
    :param keep_last_filters: List of filter columns, where entries should not be deleted; Default: None
    :param keep_last_group_by: Group by column/s
    :param func_filter_by_column: Additional filter by column
    :return:
    """
    try:
        if keep_last_group_by:
            if not isinstance(keep_last_group_by, list):
                keep_last_group_by = [keep_last_group_by]

            # get max record of group by column/s and delete all other records
            # SubQuery is not used here to make it work with SQLite and MS Sql
            subq = (
                session.query(
                    func.max(airflow_db_model.id).label("max_id")
                )
                .group_by(*keep_last_group_by)
                .subquery()
            )

            query = (
                session.query(airflow_db_model)
                .filter(
                    and_(
                        getattr(airflow_db_model, 'id') != subq.c.max_id,
                        getattr(airflow_db_model, age_check_column) <= (
                                now() - timedelta(days=age_in_days)
                        ),
                    )
                )
            )
        else:
            query = (
                session.query(airflow_db_model)
                .filter(
                    getattr(airflow_db_model, age_check_column) <= (
                            now() - timedelta(days=age_in_days)
                    )
                )
            )

        if keep_last_filters:
            for filter_column in keep_last_filters:
                query = query.filter(filter_column)

        if func_filter_by_column:
            query = query.filter(func_filter_by_column)

        objects_to_delete = query.all()

        if print_deletes:
            if objects_to_delete:
                logging.info(
                    "Cleaning up the following objects from {}:".format(airflow_db_model.__tablename__)
                )
                for entry in objects_to_delete:
                    logging.info("ID - " + str(entry.id) + ", Last Updated - " + str(entry))
            else:
                logging.info(
                    "No objects to clean up from {}.".format(airflow_db_model.__tablename__)
                )

        if enable_delete:
            query.delete(synchronize_session=False)
            session.commit()
            logging.info(
                "Cleaned up {} objects from {}.".format(
                    len(objects_to_delete), airflow_db_model.__tablename__
                )
            )
        else:
            logging.info("Skipping deletion process...")

    except ProgrammingError:
        logging.exception("Error cleaning up the db entries for - {}".format(airflow_db_model.__tablename__))


for db_object in DATABASE_OBJECTS:
    cleanup = PythonOperator(
        task_id='cleanup_{}'.format(db_object["airflow_db_model"].__tablename__),
        python_callable=cleanup_callable,
        op_kwargs={
            'session': settings.Session(),
            'age_in_days': DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS,
            'print_deletes': PRINT_DELETES,
            'enable_delete': ENABLE_DELETE,
            'airflow_db_model': db_object["airflow_db_model"],
            'age_check_column': db_object["age_check_column"],
            'keep_last': db_object["keep_last"],
            'keep_last_filters': db_object["keep_last_filters"],
            'keep_last_group_by': db_object["keep_last_group_by"],
        },
        provide_context=True,
        dag=dag,
    )

    cleanup.set_upstream(print_configuration)

dag.doc_md = __doc__
