Overview
---
This project is to build data lake with spark, we need to build pipeline to extract data from s3, transofrm it with spark and load it back to s3.

This is the database schema design:

![DB Schema](SCHEMA.png)

ETL Pipeline:
---
1. create spark session
```
spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
```
2. Load song data from s3 to local spark. Then extract related columns for song table and artist table and save the parquet back to s3.

this code is used to load song data from s3:
```
df = spark.read.json(input_data+"song_data/*/*/*/*.json")
    df.createOrReplaceTempView("songs")
```

this code is used to extract proper columns to create songs table:
```
songs_table = spark.sql("""
    SELECT distinct song_id, title, artist_id, year, duration
    FROM songs
""")
```

this code is used to load songs table back to s3:
```
songs_table.write.partitionBy("year", "artist_id").parquet(output_data+"tables/songs", 'overwrite')
```

3. Load logs data from s3 to local spark, Then extract related columns for users table, time table and songplays table

How to Run
---
In the terminal:
1. run python3 etl.py to trigger the extract data from aws s3, process the data with spark and load it back to s3