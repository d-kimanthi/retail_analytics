spark.sql("CREATE DATABASE IF NOT EXISTS my_catalog.raw")

spark.sql("""
CREATE TABLE my_catalog.raw.pos_transactions (
  store_id INT,
  product STRING,
  quantity INT,
  price DOUBLE,
  total_amount DOUBLE,
  timestamp STRING,
  event_time TIMESTAMP
)
PARTITIONED BY (days(event_time))
""")



spark.sql("CREATE DATABASE IF NOT EXISTS my_catalog.marts")

spark.sql("""
CREATE TABLE my_catalog.marts.daily_sales_summary (
  store_id INT,
  product STRING,
  sale_date DATE,
  total_quantity BIGINT,
  total_sales DOUBLE
)
PARTITIONED BY (sale_date)
""")
