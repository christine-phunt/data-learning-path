import psycopg2
import psycopg2.extras

hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = '1234'
port_id = 5432
conn = None

try:
    with psycopg2.connect(
                host = hostname,
                dbname = database,
                user = username,
                password = pwd,
                port = port_id) as conn:

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute('DROP TABLE IF EXISTS artists')

            create_artist_table = ''' CREATE TABLE artists (
    -- More about identity column:
    -- https://www.2ndquadrant.com/en/blog/postgresql-10-identity-columns/
    -- https://www.depesz.com/2017/04/10/waiting-for-postgresql-10-identity-columns/
    artist_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL
);
 '''
            cur.execute(create_artist_table)

            cur.execute('DROP TABLE IF EXISTS albums')

            create_album_table = ''' CREATE TABLE albums (
    album_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    artist_id INTEGER REFERENCES artists(artist_id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    released DATE NOT NULL
);
 '''
            cur.execute(create_album_table)

            cur.execute('DROP TABLE IF EXISTS genres')

            create_genres_table = ''' CREATE TABLE genres (
    genre_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL
);
 '''
            cur.execute(create_genres_table)

            cur.execute('DROP TABLE IF EXISTS genres')

            create_genres_table = ''' CREATE TABLE genres (
    genre_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL
);
 '''
            cur.execute(create_genres_table)


            cur.execute('DROP TABLE IF EXISTS album_genres')

            create_album_genres_table = ''' CREATE TABLE album_genres (
    album_id INTEGER REFERENCES albums(album_id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (album_id, genre_id)
);
 '''
            cur.execute(create_album_genres_table)



            
except Exception as error:
    print(error)
finally:
    if conn is not None:
        conn.close()