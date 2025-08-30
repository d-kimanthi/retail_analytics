from dagster import ScheduleDefinition
from ..jobs.batch_mart_pipeline import build_sales_mart_job

daily_sales_schedule = ScheduleDefinition(
    job=build_sales_mart_job,
    cron_schedule="0 1 * * *",  # Every day at 1 AM
    name="daily_sales_schedule",
)
