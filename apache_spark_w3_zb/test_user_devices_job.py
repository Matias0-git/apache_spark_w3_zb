import datetime
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DateType
from pyspark.sql.functions import to_date, lit
from src.jobs.user_devices_job import process_user_devices_data

def test_process_user_devices_data(spark):

    events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("event_time", StringType(), True)
    ])
    devices_schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("browser_type", StringType(), True)
    ])

    input_events_data = [
        ("user1", "deviceA", "2023-01-04 10:00:00"),
        ("user1", "deviceB", "2023-01-04 11:30:00"),
        ("user2", "deviceC", "2023-01-04 09:00:00"),
        ("user1", "deviceA", "2023-01-04 10:05:00"),
        ("user3", "deviceD", "2023-01-04 14:00:00"),
        ("user4", "deviceE", "2023-01-03 15:00:00"),
        (None, "deviceF", "2023-01-04 16:00:00"),
        ("user5", None, "2023-01-04 17:00:00")
    ]
    input_devices_data = [
        ("deviceA", "Chrome"),
        ("deviceB", "Firefox"),
        ("deviceC", "Edge"),
        ("deviceD", "Safari"),
        ("deviceE", "Opera"),
        ("deviceF", "Brave")
    ]

    events_df = spark.createDataFrame(input_events_data, events_schema)
    devices_df = spark.createDataFrame(input_devices_data, devices_schema)

    expected_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("browser_types_used", ArrayType(StringType()), True),
        StructField("device_activity_datelist", ArrayType(DateType()), True)
    ])

    expected_data = [
        ("user1", datetime.date(2023, 1, 4), ["Chrome", "Firefox"], [datetime.date(2023, 1, 4)]),
        ("user2", datetime.date(2023, 1, 4), ["Edge"], [datetime.date(2023, 1, 4)]),
        ("user3", datetime.date(2023, 1, 4), ["Safari"], [datetime.date(2023, 1, 4)])
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    execution_date_str = "2023-01-04"
    result_df = process_user_devices_data(spark, events_df, devices_df, execution_date_str)

    assert result_df.sort("user_id").collect() == expected_df.sort("user_id").collect()
