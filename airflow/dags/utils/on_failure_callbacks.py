import os
from airflow.providers.slack.notifications.slack import send_slack_notification


def get_on_failure_callbacks():
    callbacks = []

    channel = os.environ.get('SLACK_NOTIFICATION_CHANNEL')
    if channel:
        callbacks.append([
            send_slack_notification(
                text="""
                    :red_circle: Airflow Task Failed.
                    *Task*: {{ ti.task_id }}
                    *Dag*: {{ dag.dag_id }}
                    *Time*: {{ logical_date.strftime("%Y-%m-%d %H:%M:%S %Z") }}
                    *Log Url*: <{{ ti.log_url }}|Logs>
                """,
                channel=channel,
            )
        ])
    return callbacks


failure_callbacks = get_on_failure_callbacks()
