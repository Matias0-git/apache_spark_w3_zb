import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, cast, array, to_date, date_sub, coalesce, array_distinct, sort_array, when
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType

def create_spark_session(app_name="HostsCumulatedJob", master="local[*]"):
    """Initializes and returns a SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()

def process_hosts_cumulated_data(spark, events_df, hosts_cumulated_df_yesterday, execution_date_str):
    """
    Incrementally populates hosts_cumulated table by merging current day activity
    and previous activity of the hosts.
    Corresponds to HW week 2 Task 6.
    """
    _execution_date_ = to_date(lit(execution_date_str))
    _yesterday_date_ = date_sub(_execution_date_, 1)

    today_host_activity = events_df.filter(
        (col("event_time").cast("date") == _execution_date_) &
        (col("host").isNotNull())
    ).groupBy(
        col("host")
    ).agg(
        _execution_date_.alias("activity_date_daily")
    )

    yesterday_cumulated = hosts_cumulated_df_yesterday.filter(
        col("date") == _yesterday_date_
    ).select(
        col("host"),
        col("host_activity_datelist"),
        col("date")
    )

    final_df = today_host_activity.join(
        yesterday_cumulated,
        on=today_host_activity.host == yesterday_cumulated.host,
        how="fullouter"
    ).select(
        coalesce(today_host_activity.host, yesterday_cumulated.host).alias("host"),
        sort_array(array_distinct(
            F.concat(
                coalesce(yesterday_cumulated.host_activity_datelist, F.array().cast(ArrayType(DateType()))),
                when(today_host_activity.activity_date_daily.isNotNull(), array(today_host_activity.activity_date_daily))
                .otherwise(F.array().cast(ArrayType(DateType())))
            )
        )).alias("host_activity_datelist"),
        _execution_date_.alias("date")
    ).filter(
        col("host").isNotNull()
    )

    return final_df

if __name__ == "__main__":
    spark = create_spark_session()

    events_schema = StructType([
        StructField("host", StringType(), True),
        StructField("event_time", StringType(), True)
    ])
    hosts_cumulated_schema = StructType([
        StructField("host", StringType(), True),
        StructField("host_activity_datelist", ArrayType(DateType()), True),
        StructField("date", DateType(), True)
    ])


    sample_events_data = [
        ("hostA", "2023-01-04 10:00:00"),
        ("hostB", "2023-01-04 11:00:00"),
        ("hostA", "2023-01-04 12:00:00"),
        ("hostC", "2023-01-04 13:00:00"),
        (None, "2023-01-04 14:00:00")
    ]
    events_df = spark.createDataFrame(sample_events_data, events_schema)

    sample_hosts_cumulated_yesterday_data = [
        ("hostA", [to_date(lit("2023-01-02")), to_date(lit("2023-01-03"))], to_date(lit("2023-01-03"))),
        ("hostB", [to_date(lit("2023-01-01")), to_date(lit("2023-01-03"))], to_date(lit("2023-01-03"))),
        ("hostD", [to_date(lit("2023-01-03"))], to_date(lit("2023-01-03")))
    ]
    hosts_cumulated_df_yesterday = spark.createDataFrame(sample_hosts_cumulated_yesterday_data, hosts_cumulated_schema)

    execution_date = "2023-01-04"
    result_df = process_hosts_cumulated_data(spark, events_df, hosts_cumulated_df_yesterday, execution_date)

    print(f"Result for hosts_cumulated on {execution_date}:")
    result_df.show(truncate=False)

    spark.stop()
