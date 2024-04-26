# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
from pyspark.sql import SparkSession
#create session
spark=SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType,DecimalType
from pyspark.sql.functions import *

# Define the schema using StructType and StructField
ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# Now you can use this schema to read the CSV file with PySpark
# For example:
# df = spark.read.csv("Ball_By_Ball.csv", schema=ball_by_ball_schema)


# COMMAND ----------

ball_by_ball_df = spark. read.schema (ball_by_ball_schema).format("csv").option("header", "true").load("s3://ipl-data-analysis-project/Ball_By_Ball.csv")

# COMMAND ----------

# Define the schema using StructType and StructField
match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

#ball_by_ball_df = spark. read.schema (ball_by_ball_schema).format("csv").option("header", "true").load("s3://ipl-data-analysis-project/Ball_By_Ball.csv")
match_df = spark.read.schema (match_schema).format("csv").option("header", "true").load("s3://ipl-data-analysis-project/Match.csv")

# Now you can use this schema to read the CSV file with PySpark
# For example:
# df = spark.read.csv("match.csv", schema=match_schema)


# COMMAND ----------

# Define the schema using StructType and StructField
player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

player_df = spark.read.schema (player_schema).format("csv").option("header", "true").load("s3://ipl-data-analysis-project/Player.csv")

# COMMAND ----------

# Define the schema using StructType and StructField
player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(38, 0), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

player_match_df = spark.read.schema (player_match_schema).format("csv").option("header", "true").load("s3://ipl-data-analysis-project/Player_match.csv")

# COMMAND ----------

# Define the schema using StructType and StructField
team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

team_df = spark.read.schema (team_schema).format("csv").option("header", "true").load("s3://ipl-data-analysis-project/Team.csv")

# COMMAND ----------

# Filter to include only valid deliveries (excluding extras like wides and no balls for specific analyses)
ball_by_ball_df = ball_by_ball_df.filter((col("wides") == 0) & (col("noballs")==0))

# Aggregation: Calculate the total and average runs scored in each match and inning
total_and_avg_runs = ball_by_ball_df.groupBy("match_id", "innings_no").agg(
sum("runs_scored").alias ("total_runs"),
avg("runs_scored").alias ("average_runs")
)

# COMMAND ----------

total_and_avg_runs.display()

# COMMAND ----------

# Group the data by player and sum up the scores for each player
total_scores_by_player = (ball_by_ball_df.join(player_match_df, 'match_id')
                          .groupBy("player_id", "player_name")
                          .agg(sum("runs_scored").alias("total_score"))
                          .orderBy("total_score", ascending=False)
                          .limit(10))

# COMMAND ----------

total_scores_by_player.display()

# COMMAND ----------

# Group the data by player and match, then calculate the maximum score for each player in each match
highest_score_in_single_match =(ball_by_ball_df.join(player_match_df, 'match_id') \
    .groupBy("player_id", "player_name")
                          .agg(max("runs_scored").alias("highest_score"))
                          .orderBy("highest_score", ascending=False)
                          .limit(20))

# COMMAND ----------

highest_score_in_single_match.display()

# COMMAND ----------

# Group the data by bowler and count the occurrences of wickets for each bowler
total_wickets_by_bowler = (ball_by_ball_df.filter(ball_by_ball_df["out_type"].isNotNull())
                             .join(player_match_df, 'match_id')
                           .groupBy("bowler","player_name")
                           .agg(count("out_type").alias("total_wickets"))
                           .orderBy("total_wickets", ascending=False)
                           .limit(20))

# COMMAND ----------

total_wickets_by_bowler.display()

# COMMAND ----------

# Group the data by player, then count the occurrences of wickets for each player in the specified match
highest_wickets_in_single_match = (ball_by_ball_df.filter(ball_by_ball_df["out_type"].isNotNull())
                                   .join(player_match_df, 'match_id')
                                   .groupBy("player_id", "player_name")
                                   .agg(count("out_type").alias("wickets_in_match"))
                                   .groupBy("player_id", "player_name")
                                   .agg(max("wickets_in_match").alias("highest_wickets_in_single_match")))


# COMMAND ----------

highest_wickets_in_single_match.display()

# COMMAND ----------


