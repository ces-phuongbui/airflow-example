import uuid
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.utils.email import send_email
import requests
from airflow.operators.email_operator import EmailOperator


url = "https://potterapi-fedeperin.vercel.app/en/books"


# Customize the email send failure
def on_failure_callback(context):
    ti: TaskInstance = context["ti"]
    dag_id = ti.dag_id
    task_id = ti.task_id

    log_url = context["task_instance"].log_url
    error_message = f"Task {task_id} in DAG {dag_id} failed.\n\nLog URL: {log_url}"

    send_email(
        to="phuong.bui@codeenginestudio.com",
        subject=f"Task {task_id} Failed in DAG {dag_id}",
        html_content=f"<p>{error_message}</p>",
    )


default_args = {
    "owner": "Barrie",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
    "email": "recipient_email",
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": True,
    # "on_failure_callback": on_failure_callback,    Custom email
}


@dag(
    default_args=default_args,
    dag_id="ETL_data_from_api",
    schedule_interval="39 10 * * *",
    start_date=datetime(2024, 11, 13),
)
def task_flow_api(**kwargs):
    @task()
    def extract():
        try:
            response = requests.get(url)
            print("data: ", response.json())
            print("current date:", datetime.now())
            return response.json()
        except:
            raise Exception("Call api error!")

    @task()
    def transform(data_dict):
        transformed_data = []

        for item in data_dict:
            new_item = {"id": str(uuid.uuid4()), "name": item.pop("title")}
        new_item.update(item)
        transformed_data.append(new_item)

        return transformed_data

    @task()
    def load(result_from_transform):

        print("load result_from_transform: ", result_from_transform)

    data = extract()
    order_summary = transform(data)
    send_email = EmailOperator(
        task_id="send_email_on_success",
        to="recipient_email",
        subject="ETL DAG Success",
        html_content="<p>The ETL DAG ETL_api_success has completed successfully.</p>",
    )
    load(order_summary) >> send_email


task_flow_api = task_flow_api()
