# DROP TABLES

user_table_drop = "DROP TABLE IF EXISTS T_USER"
song_table_drop = "DROP TABLE IF EXISTS T_SONG"
artist_table_drop = "DROP TABLE IF EXISTS T_ARTIST"
time_table_drop = "DROP TABLE IF EXISTS T_TIME_TABLE"
songplay_table_drop = "DROP TABLE IF EXISTS T_SONG_PLAY"

# CREATE TABLES

user_table_create = ("""
 CREATE TABLE T_USER(user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar)
""")

song_table_create = ("""
 CREATE TABLE T_SONG(song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float)
""")

artist_table_create = ("""
 CREATE TABLE T_ARTIST(artist_id varchar PRIMARY KEY, artist_name varchar, location varchar, latitude float, longitude float)
""")

time_table_create = ("""
 CREATE TABLE T_TIME_TABLE(start_time timestamp, day int, week int, month int, year int, weekday int)
""")

songplay_table_create = ("""
 CREATE TABLE T_SONG_PLAY(songplay_id SERIAL PRIMARY KEY,start_time timestamp NOT NULL, user_id int NOT NULL, \
 level varchar,song_id varchar, artist_id varchar, session_id int NOT NULL, location varchar, user_agent varchar,
 CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES T_USER(user_id), CONSTRAINT fk_song FOREIGN KEY (song_id) \
 REFERENCES T_SONG(song_id),CONSTRAINT fk_artist FOREIGN KEY (artist_id) REFERENCES T_ARTIST(artist_id) ,\
 CONSTRAINT unq_time_user_session UNIQUE (start_time, user_id, session_id))
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO T_SONG_PLAY(start_time, user_id, level, song_id, \
artist_id, session_id, location, user_agent) \
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO T_USER(user_id, first_name, last_name, gender, level) \
VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (user_id) DO UPDATE
SET level = EXCLUDED.level;    
""")

song_table_insert = ("""
INSERT INTO T_SONG(song_id, title, artist_id, year, duration) \
VALUES (%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
INSERT INTO T_ARTIST(artist_id, latitude, location, longitude , artist_name) \
VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO T_TIME_TABLE(start_time, day, week, month, year, weekday) \
VALUES (%s, %s, %s, %s, %s, %s) \
ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = (""" 
select T_SONG.song_id,T_ARTIST.artist_id from T_SONG JOIN T_ARTIST ON \
T_SONG.artist_id=T_ARTIST.artist_id where T_SONG.title = %s and T_ARTIST.artist_name = %s and T_SONG.duration = %s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create,
                        time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
