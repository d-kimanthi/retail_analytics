from .jobs.batch_mart_pipeline import (
    build_sales_mart_job,
)

from .schedules.daily_sales_schedule import (
    daily_sales_schedule,
)


# This tells Dagster which jobs are available
defs = [build_sales_mart_job, daily_sales_schedule]
