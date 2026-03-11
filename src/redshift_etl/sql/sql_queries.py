from redshift_etl.scripts.config_helper import get_config

# CONFIG
config = get_config()
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
ROLE_ARN = config.get('IAM_ROLE', 'ROLE_ARN')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_data;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS events_data (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession SMALLINT,
        lastName VARCHAR,
        length DOUBLE PRECISION,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration DOUBLE PRECISION,
        sessionId INTEGER,
        song VARCHAR,
        status SMALLINT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_data (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DOUBLE PRECISION,
        year SMALLINT
    );
""")


# Fact Table: songplays

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id INTEGER NOT NULL REFERENCES users(user_id) DISTKEY,
        level VARCHAR(10),
        song_id VARCHAR REFERENCES songs(song_id),
        artist_id VARCHAR REFERENCES artists(artist_id),
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    );
""")


# Dimension Tables: users, songs, artists, time

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY NOT NULL SORTKEY DISTKEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        gender VARCHAR(10),
        level VARCHAR(10)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY NOT NULL SORTKEY,
        title VARCHAR,
        artist_id VARCHAR REFERENCES artists(artist_id),
        year SMALLINT,
        duration DECIMAL(10,5)
    )
    DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY NOT NULL SORTKEY,
        name VARCHAR(255),
        location VARCHAR,
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6)
    )
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY NOT NULL SORTKEY,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
    )
    DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY events_data FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {}
    region 'us-west-2';
""").format(LOG_DATA, ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY song_data FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + ed.ts/1000 * INTERVAL '1 second' AS start_time,
                    ed.userId AS user_id,
                    ed.level AS level,
                    sd.song_id AS song_id,
                    sd.artist_id AS artist_id,
                    ed.sessionId AS session_id,
                    ed.location AS location,
                    ed.userAgent AS user_agent
    FROM events_data AS ed
    JOIN song_data AS sd
        ON ed.song = sd.title
        AND ed.artist = sd.artist_name
        AND ed.length = sd.duration
    WHERE ed.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  user_id, 
            first_name, 
            last_name, 
            gender, 
            level
    FROM (
        SELECT userId AS user_id,
               firstName AS first_name,
               lastName AS last_name,
               gender,
               level,
               ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS row_num
        FROM events_data
        WHERE userId IS NOT NULL 
          AND page = 'NextSong'
    )
    WHERE row_num = 1;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  song_id,
            title, 
            artist_id, 
            year,
            duration
    FROM (
        SELECT song_id,
               title,
               artist_id,
               year,
               duration,
               ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY year DESC) AS row_num
        FROM song_data
        WHERE song_id IS NOT NULL
    )
    WHERE row_num = 1;
""")


artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  artist_id,
            name,
            location,
            latitude,
            longitude
    FROM (
        SELECT  artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude::DECIMAL(9,6) AS latitude,
                artist_longitude::DECIMAL(9,6) AS longitude,
                ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_name) AS row_num
        FROM song_data
        WHERE artist_id IS NOT NULL
    )
    WHERE row_num = 1;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
                    EXTRACT(HOUR FROM start_time) AS hour,
                    EXTRACT(DAY FROM start_time) AS day,
                    EXTRACT(WEEK FROM start_time) AS week,
                    EXTRACT(MONTH FROM start_time) AS month,
                    EXTRACT(YEAR FROM start_time) AS year,
                    EXTRACT(DOW FROM start_time) AS weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
