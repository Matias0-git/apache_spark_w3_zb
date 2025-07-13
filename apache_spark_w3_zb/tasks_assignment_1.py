from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
import os
import shutil

MATCH_DETAILS_SCHEMA = StructType([
    StructField("match_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True)
])

MATCHES_SCHEMA = StructType([
    StructField("match_id", StringType(), True),
    StructField("playlist_name", StringType(), True),
    StructField("map_id", StringType(), True),
    StructField("game_duration_seconds", IntegerType(), True)
])

MEDALS_MATCHES_PLAYERS_SCHEMA = StructType([
    StructField("match_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("medal_id", StringType(), True),
    StructField("medal_count", IntegerType(), True)
])

MEDALS_SCHEMA = StructType([
    StructField("medal_id", StringType(), True),
    StructField("medal_name", StringType(), True)
])

MAPS_SCHEMA = StructType([
    StructField("map_id", StringType(), True),
    StructField("map_name", StringType(), True)
])


def create_spark_session(app_name="task1", auto_broadcast_threshold="-1", master="local[*]"): # bullet point 1 addressed
    """Initializes and returns a SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.autoBroadcastJoinThreshold", auto_broadcast_threshold) \
        .master(master) \
        .getOrCreate()

def load_dataframes(spark, base_path=".", use_bucketed_tables=False):
    if use_bucketed_tables:
        df_match_details = spark.table("match_details_bucketed")
        df_matches = spark.table("matches_bucketed")
        df_medals_matches_players = spark.table("medals_matches_players_bucketed")
    else:
        df_match_details = spark.read.csv(os.path.join(base_path, "match_details.csv"), header=True, inferSchema=True, schema=MATCH_DETAILS_SCHEMA)
        df_matches = spark.read.csv(os.path.join(base_path, "matches.csv"), header=True, inferSchema=True, schema=MATCHES_SCHEMA)
        df_medals_matches_players = spark.read.csv(os.path.join(base_path, "medals_matches_players.csv"), header=True, inferSchema=True, schema=MEDALS_MATCHES_PLAYERS_SCHEMA)

    df_medals = spark.read.csv(os.path.join(base_path, "medals.csv"), header=True, inferSchema=True, schema=MEDALS_SCHEMA)
    df_maps = spark.read.csv(os.path.join(base_path, "maps.csv"), header=True, inferSchema=True, schema=MAPS_SCHEMA)
    df_medals.cache()
    df_maps.cache()

    return df_match_details, df_matches, df_medals_matches_players, df_medals, df_maps

def bucket_and_persist_data(df_match_details, df_matches, df_medals_matches_players, num_buckets=16): # bullet point 3 addressed
    """
    Persists large DataFrames as bucketed tables for optimized joins.
    Ensures a clean state by dropping tables first.
    """
    spark = df_match_details.sparkSession
    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {spark.catalog.currentDatabase()}")


    print("Bucketing match_details...")
    df_match_details \
        .write.format("parquet") \
        .mode("overwrite") \
        .bucketBy(num_buckets, "match_id") \
        .sortBy("match_id") \
        .saveAsTable("match_details_bucketed")

    print("Bucketing matches...")
    df_matches \
        .write.format("parquet") \
        .mode("overwrite") \
        .bucketBy(num_buckets, "match_id") \
        .sortBy("match_id") \
        .saveAsTable("matches_bucketed")

    print("Bucketing medals_matches_players...")
    df_medals_matches_players \
        .write.format("parquet") \
        .mode("overwrite") \
        .bucketBy(num_buckets, "match_id") \
        .sortBy("match_id") \
        .saveAsTable("medals_matches_players_bucketed")

def perform_joins(df_match_details, df_matches, df_medals_matches_players, df_medals, df_maps): # bullet point 2 addressed

    df_joined = df_match_details.join(
        df_matches,
        on="match_id",
        how="inner"
    )

    df_joined = df_joined.join(
        df_medals_matches_players,
        on=["match_id", "player_id"],
        how="left_outer"
    )

    df_joined = df_joined.join(
        broadcast(df_medals),
        on="medal_id",
        how="left_outer"
    )

    df_joined = df_joined.join(
        broadcast(df_maps),
        on="map_id",
        how="inner"
    )

    df_final = df_joined.select(
        col("match_id"),
        col("player_id"),
        col("kills"),
        col("deaths"),
        col("assists"),
        col("playlist_name"),
        col("map_name"),
        col("medal_name"),
        col("medal_count")
    )
    df_final.cache()
    return df_final

def calculate_aggregations(df_final): # bullet point 4 addressed
    """Calculates all required aggregated metrics."""

    player_kills_avg = df_final.groupBy("player_id") \
        .agg(avg("kills").alias("avg_kills_per_game")) \
        .orderBy(col("avg_kills_per_game").desc())

    playlist_most_played = df_final.groupBy("playlist_name") \
        .agg(count("match_id").alias("total_matches_played")) \
        .orderBy(col("total_matches_played").desc())

    map_most_played = df_final.groupBy("map_name") \
        .agg(count("match_id").alias("total_matches_played")) \
        .orderBy(col("total_matches_played").desc())

    killing_spree_medals = df_final.filter(col("medal_name").like("%Killing Spree%"))
    map_killing_spree = killing_spree_medals.groupBy("map_name") \
        .agg(sum("medal_count").alias("total_killing_spree_medals")) \
        .orderBy(col("total_killing_spree_medals").desc())

    return player_kills_avg, playlist_most_played, map_most_played, map_killing_spree

def explore_partition_sorting(df_final, num_buckets=16): # here we play with sortWithPartitions
    """Explores different sortWithinPartitions strategies."""
    df_sorted_playlist = df_final.repartition(num_buckets, "playlist_name") \
                                 .sortWithinPartitions("playlist_name")
    print("Explain plan for sorting by playlist_name:")
    df_sorted_playlist.explain(True)

    df_sorted_map = df_final.repartition(num_buckets, "map_name") \
                            .sortWithinPartitions("map_name")
    print("\nExplain plan for sorting by map_name:")
    df_sorted_map.explain(True)

    df_sorted_player = df_final.repartition(num_buckets, "player_id") \
                               .sortWithinPartitions("player_id")
    print("\nExplain plan for sorting by player_id:")
    df_sorted_player.explain(True)


def run_game_analytics_job(spark, data_path=".", num_buckets=16):
    """Main function to run the entire game analytics job."""

    spark.catalog.dropTempView("match_details_bucketed")
    spark.catalog.dropTempView("matches_bucketed")
    spark.catalog.dropTempView("medals_matches_players_bucketed")


    df_match_details, df_matches, df_medals_matches_players, df_medals, df_maps = \
        load_dataframes(spark, base_path=data_path, use_bucketed_tables=False)

    bucket_and_persist_data(df_match_details, df_matches, df_medals_matches_players, num_buckets)

    df_match_details_bucketed = spark.table("match_details_bucketed")
    df_matches_bucketed = spark.table("matches_bucketed")
    df_medals_matches_players_bucketed = spark.table("medals_matches_players_bucketed")



    df_final = perform_joins(
        df_match_details_bucketed,
        df_matches_bucketed,
        df_medals_matches_players_bucketed,
        df_medals,
        df_maps
    )


    player_kills_avg, playlist_most_played, map_most_played, map_killing_spree = \
        calculate_aggregations(df_final)

    explore_partition_sorting(df_final, num_buckets)

    return player_kills_avg, playlist_most_played, map_most_played, map_killing_spree

if __name__ == "__main__":

    spark = create_spark_session(app_name="GameAnalytics", auto_broadcast_threshold="-1", master="local[*]")

    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    if os.path.exists(warehouse_dir):
        print(f"Cleaning up existing warehouse directory: {warehouse_dir}")
        shutil.rmtree(warehouse_dir)

    player_kills_avg, playlist_most_played, map_most_played, map_killing_spree = \
        run_game_analytics_job(spark, data_path=".")

    print("\n--- Player Average Kills ---")
    player_kills_avg.show(5, truncate=False)

    print("\n--- Most Played Playlist ---")
    playlist_most_played.show(5, truncate=False)

    print("\n--- Most Played Map ---")
    map_most_played.show(5, truncate=False)

    print("\n--- Map with Most Killing Spree Medals ---")
    map_killing_spree.show(5, truncate=False)

    spark.stop()
    print("Spark job finished.")
