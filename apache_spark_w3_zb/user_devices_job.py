import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, cast, array, to_date, date_sub, coalesce, array_distinct, sort_array, when
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType

def create_spark_session(app_name="UserDevicesJob", master="local[*]"):
    """Initializes and returns a SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()

def process_user_devices_data(spark, events_df, devices_df, execution_date_str):
    """
    Processes user device activity to identify distinct browser types and activity dates.
    Corresponds to HW week 2 Task 3.
    """
    execution_date = to_date(lit(execution_date_str))

    today_raw_activity = events_df.join(
        devices_df,
        on=events_df.device_id == devices_df.device_id,
        how="inner"
    ).filter(
        (col("event_time").cast("date") == execution_date) &
        (col("user_id").isNotNull()) &
        (col("browser_type").isNotNull())
    ).select(
        col("user_id").cast(StringType()).alias("user_id"),
        col("browser_type"),
        col("event_time").cast("date").alias("event_date")
    )

    today_aggregated_activity = today_raw_activity.groupBy(
        col("user_id"),
        col("event_date").alias("date")
    ).agg(
        sort_array(array_distinct(F.collect_list(col("browser_type")))).alias("browser_types_used"),
        array(col("event_date")).alias("device_activity_datelist")
    )

    return today_aggregated_activity

if __name__ == "__main__":
    spark = create_spark_session()

    events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("event_time", StringType(), True)
    ])
    devices_schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("browser_type", StringType(), True)
    ])

    sample_events_data = [
        ("user1", "deviceA", "2023-01-04 10:00:00"),
        ("user1", "deviceB", "2023-01-04 11:00:00"),
        ("user2", "deviceC", "2023-01-04 12:00:00"),
        ("user1", "deviceA", "2023-01-04 13:00:00"),
        ("user3", "deviceD", "2023-01-04 14:00:00"),
        ("user4", "deviceE", "2023-01-03 15:00:00"),
        (None, "deviceF", "2023-01-04 16:00:00"),
        ("user5", None, "2023-01-04 17:00:00")
    ]
    events_df = spark.createDataFrame(sample_events_data, events_schema)

    sample_devices_data = [
        ("deviceA", "Chrome"),
        ("deviceB", "Firefox"),
        ("deviceC", "Edge"),
        ("deviceD", "Safari"),
        ("deviceE", "Opera"),
        ("deviceF", "Brave")
    ]
    devices_df = spark.createDataFrame(sample_devices_data, devices_schema)

    execution_date = "2023-01-04"
    result_df = process_user_devices_data(spark, events_df, devices_df, execution_date)

    print(f"Result for user_devices_cumulated on {execution_date}:")
    result_df.show(truncate=False)

    spark.stop()
