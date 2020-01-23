import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StringType
import  pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["AWS_ACCESS_KEY_ID"]= ""
os.environ["AWS_SECRET_ACCESS_KEY"]= ""

def create_spark_session():
    """
    This query will be responsible for creating a spark session instance

    Parameters:
    None

    Returns:
    instance of SparkSession
   """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    This query will be responsible for downloading song data from s3, extract columns to create songs table and save it back to s3

    Parameters:
    None

    Returns:
    instance of SparkSession
   """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data")
    
    # read song data file
    df = spark.read.json(input_data+"song_data/*/*/*/*.json")
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT distinct song_id, title, artist_id, year, duration
    FROM songs
""")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+"tables/songs", 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT distinct artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude longitude FROM songs
""")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"tables/artists", "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This query will be responsible for downloading log data from s3, extract columns to create logs table, users table and songplays table, then save it back to s3

    Parameters:
    spark: spark session instance
    input_data: s3 input data folder
    output: s3 output data folder

    Returns:
    instance of SparkSession
   """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data")

    # read log data file
    df = spark.read.json(input_data+"log-data/*.json")
    
    # filter by actions for song plays
    df = df.filter(df["page"] != "NextSong")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp('ts'))
    
    # create datetime column from original timestamp column
#     get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000), StringType())
#     df = df.withColumn("datetime", get_datetime('ts'))
    
    df.createOrReplaceTempView("logs")
    
    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender, level, location, userAgent
    FROM logs
""")
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"tables/users", "overwrite")

    # extract columns to create time table
    time_table = spark.sql("""
    SELECT 
        start_time, 
        hour(start_time) AS hour,
        day(start_time) AS day,
        weekofyear(start_time) AS week,
        month(start_time) AS month,
        year(start_time) AS year, 
        weekday(start_time) AS weekday 
    FROM (
        SELECT DISTINCT cast(ts/1000 as TIMESTAMP) as start_time 
        FROM logs     
        )
    """)
    time_table.createOrReplaceTempView("time_table")

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data+"tables/time", 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+"song_data/*/*/*/*.json")
    song_df.createOrReplaceTempView("songs")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT logs.start_time, time_table.year, time_table.month, userId as user_id, level, song_id, artist_id, sessionId as session_id, location, userAgent
    FROM logs left join songs on (logs.song = songs.title)
    join time_table on (time_table.start_time = logs.start_time)
""")
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data+"tables/songplays", 'overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://micudacity1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
