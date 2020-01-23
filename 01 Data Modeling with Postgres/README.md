Overview
---
This is the first project in data engineer pgrogram to practice the data modeling skills for sql datbase with postgres. It parses customer's existing json format data and store it into postgres database to optimize the queries. One pipeline will be built to import the data and test it.

This is the database schema design and a star schema has been used:

![DB Schema](schema.png)

ETL Pipeline:
---
1. read song data from data/song_data directory, convert them from json to DataFrame, extract the song related info and insert them to songs table. The code is as follow:
```
df = pd.read_json(filepath, lines=True)
# insert song record
song_data = df[['song_id', 'title', 'artist_id','year', 'duration']].values[0]
cur.execute(song_table_insert, song_data)
```
2. extract the artist info from the above DataFrame and insert them into artists table
```
artist_data = df[['artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude']].values[0]
cur.execute(artist_table_insert, artist_data)
```
3. read log data from data/log_data directory, convert them from json to DataFrame
4. extract the ts time column and also extract related hour, day, week of year, month, year, and weekday from this epoch time bigint value.
```
# convert timestamp column to datetime
    t =pd.to_datetime(df["ts"])
    print(t.to_frame())
    # insert time data records
    time_data = [df["ts"], t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ["timestamp","hour", "day", "weekofyear", "month", "year", "weekday"]
    dft=pd.DataFrame(time_data).T
    dft.columns = column_labels
    time_df = dft
```
5.extract users information from above log data frame and insert them into users table
6.finally, get songid and artistid from song and artist tables and then insert them along with other attributes such as user_id, level, session_id etc to create the face table songplays

How to Run
---
In the terminal:
1. run python3 create_tables.py to reset the database and table
2. run python3 etl.py to trigger the import data pipeline
3. run test.ipynb jupyter file to verify the data has been inserted in to database successfully