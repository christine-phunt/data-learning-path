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
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id) as conn:

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            # cur.execute('DROP TABLE IF EXISTS employee')

            create_header = ''' TRUNCATE albums, artists, genres, album_genres;
                COPY artists FROM '/Users/ev/Documents/Data Engineering/artists.csv' CSV HEADER;
                COPY albums FROM '/Users/ev/Documents/Data Engineering/albums.csv' CSV HEADER;
                COPY genres FROM '/Users/ev/Documents/Data Engineering/genres.csv' CSV HEADER;
                COPY album_genres FROM '/Users/ev/Documents/Data Engineering/album_genres.csv' CSV HEADER; '''
            cur.execute(create_header)

            create_script = ''' 
                TRUNCATE albums, artists, genres, album_genres;
WITH _genres AS (
  INSERT INTO genres (name)
  VALUES
    ('Hip Hop'),
    ('Jazz'),
    ('Electronic'),
    ('Rock'),
    ('Pop'),
    ('Funk'),
    ('Indie')
  RETURNING *
),
_artists AS (
  INSERT INTO artists (name)
  VALUES
    ('DJ Okawari'),
    ('Steely Dan'),
    ('Missy Elliott'),
    ('TWRP'),
    ('Donald Fagen'),
    ('La Luz'),
    ('Ella Fitzgerald')
  RETURNING *
),
_albums AS (
  INSERT INTO albums (artist_id, title, released)
  VALUES
    ((SELECT artist_id FROM _artists WHERE name = 'DJ Okawari'), 'Mirror', '2009-06-24'),
    ((SELECT artist_id FROM _artists WHERE name = 'Steely Dan'), 'Pretzel Logic', '1974-02-20'),
    ((SELECT artist_id FROM _artists WHERE name = 'Missy Elliott'), 'Under Construction', '2002-11-12'),
    ((SELECT artist_id FROM _artists WHERE name = 'TWRP'), 'Return to Wherever', '2019-07-11'),
    ((SELECT artist_id FROM _artists WHERE name = 'Donald Fagen'), 'The Nightfly', '1982-10-01'),
    ((SELECT artist_id FROM _artists WHERE name = 'La Luz'), 'It''s Alive', '2013-10-15'),
    ((SELECT artist_id FROM _artists WHERE name = 'Ella Fitzgerald'), 'Pure Ella', '1994-02-15')
  RETURNING *
)
INSERT INTO album_genres (album_id, genre_id)
VALUES
  ((SELECT artist_id FROM _albums WHERE title = 'Mirror'), (SELECT genre_id FROM _genres WHERE name = 'Hip Hop')),
  ((SELECT artist_id FROM _albums WHERE title = 'Mirror'), (SELECT genre_id FROM _genres WHERE name = 'Jazz')),
  ((SELECT artist_id FROM _albums WHERE title = 'Pretzel Logic'), (SELECT genre_id FROM _genres WHERE name = 'Jazz')),
  ((SELECT artist_id FROM _albums WHERE title = 'Pretzel Logic'), (SELECT genre_id FROM _genres WHERE name = 'Rock')),
  ((SELECT artist_id FROM _albums WHERE title = 'Pretzel Logic'), (SELECT genre_id FROM _genres WHERE name = 'Pop')),
  ((SELECT artist_id FROM _albums WHERE title = 'Under Construction'), (SELECT genre_id FROM _genres WHERE name = 'Hip Hop')),
  ((SELECT artist_id FROM _albums WHERE title = 'Return to Wherever'), (SELECT genre_id FROM _genres WHERE name = 'Rock')),
  ((SELECT artist_id FROM _albums WHERE title = 'Return to Wherever'), (SELECT genre_id FROM _genres WHERE name = 'Funk')),
  ((SELECT artist_id FROM _albums WHERE title = 'The Nightfly'), (SELECT genre_id FROM _genres WHERE name = 'Jazz')),
  ((SELECT artist_id FROM _albums WHERE title = 'The Nightfly'), (SELECT genre_id FROM _genres WHERE name = 'Rock')),
  ((SELECT artist_id FROM _albums WHERE title = 'The Nightfly'), (SELECT genre_id FROM _genres WHERE name = 'Pop')),
  ((SELECT artist_id FROM _albums WHERE title = 'It''s Alive'), (SELECT genre_id FROM _genres WHERE name = 'Indie')),
  ((SELECT artist_id FROM _albums WHERE title = 'Pure Ella'), (SELECT genre_id FROM _genres WHERE name = 'Jazz'));

              
            '''
            cur.execute(create_script)


except Exception as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
