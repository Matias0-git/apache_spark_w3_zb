import datetime
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DateType
from pyspark.sql.functions import to_date, lit
from src.jobs.hosts_cumulated_job import process_hosts_cumulated_data

def test_process_hosts_cumulated_data(spark):


    events_schema = StructType([
        StructField("host", StringType(), True),
        StructField("event_time", StringType(), True)
    ])
    hosts_cumulated_schema = StructType([
        StructField("host", StringType(), True),
        StructField("host_activity_datelist", ArrayType(DateType()), True),
        StructField("date", DateType(), True)
    ])

    execution_date_str = "2023-01-04"
    yesterday_date_str = "2023-01-03"
    tomorrow_date_str = "2023-01-05"

    input_events_data = [
        ("host_X", "2023-01-04 08:00:00"),
        ("host_Y", "2023-01-04 09:00:00"),
        ("host_Y", "2023-01-04 09:30:00"),
        ("host_Z", "2023-01-04 10:00:00"),
        ("host_A", "2023-01-03 11:00:00"),
        (None, "2023-01-04 12:00:00")
    ]
    events_df = spark.createDataFrame(input_events_data, events_schema)

    input_hosts_cumulated_yesterday_data = [
        ("host_Y", [datetime.date(2023, 1, 1), datetime.date(2023, 1, 3)], datetime.date(2023, 1, 3)),
        ("host_W", [datetime.date(2023, 1, 2), datetime.date(2023, 1, 3)], datetime.date(2023, 1, 3)),
        ("host_Z", [datetime.date(2023, 1, 2)], datetime.date(2023, 1, 2)),
        ("host_V", [datetime.date(2023, 1, 3)], datetime.date(2023, 1, 3))
    ]
    hosts_cumulated_df_yesterday = spark.createDataFrame(input_hosts_cumulated_yesterday_data, hosts_cumulated_schema)

    expected_schema = StructType([
        StructField("host", StringType(), True),
        StructField("host_activity_datelist", ArrayType(DateType()), True),
        StructField("date", DateType(), True)
    ])

    expected_data = [
        ("host_X", [datetime.date(2023, 1, 4)], datetime.date(2023, 1, 4)),
        ("host_Y", [datetime.date(2023, 1, 1), datetime.date(2023, 1, 3), datetime.date(2023, 1, 4)], datetime.date(2023, 1, 4)),
        ("host_W", [datetime.date(2023, 1, 2), datetime.date(2023, 1, 3)], datetime.date(2023, 1, 4)),
        ("host_V", [datetime.date(2023, 1, 3)], datetime.date(2023, 1, 4)),
        ("host_Z", [datetime.date(2023, 1, 4)], datetime.date(2023, 1, 4))
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    result_df = process_hosts_cumulated_data(spark, events_df, hosts_cumulated_df_yesterday, execution_date_str)

    assert result_df.sort("host").collect() == expected_df.sort("host").collect()
