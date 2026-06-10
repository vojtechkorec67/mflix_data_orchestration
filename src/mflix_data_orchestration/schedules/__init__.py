from dagster import schedule
from .jobs import movies_job


@schedule(job=movies_job, cron_schedule="0 0 1 * *")
def movies_schedule():
    return {}
