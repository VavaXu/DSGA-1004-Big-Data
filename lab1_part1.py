#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
#   python lab1_part1.py music_small.db

# Ziwei Xu (Vanessa)
# NetID: zx657

import sys
import sqlite3

# The database file should be given as the first argument on the command line
# Please do not hard code the database file!
db_file = sys.argv[1]

# We connect to the database using 
with sqlite3.connect(db_file) as conn:
    # This query counts the number of artists who became active in 1990
    year = (1990,)
    for row in conn.execute('SELECT count(*) FROM artist WHERE artist_active_year_begin=?', year):
        # Since there is no grouping here, the aggregation is over all rows
        # and there will only be one output row from the query, which we can
        # print as follows:
        print('Tracks from {}: {}'.format(year[0], row[0]))
        
        # The [0] bits here tell us to pull the first column out of the 'year' tuple
        # and query results, respectively.

    # ADD YOUR CODE STARTING HERE

    # Question 1
    print('Question 1: Which artists (ids and names) contain the word Green? (Upper or lower case does not matter.)')
    
    # implement your solution to q1
    #name = ("green", )
    sql_code = "SELECT id, artist_name FROM artist WHERE lower(artist_name) LIKE ?"
    args = ['%' + 'green' + '%']
    for artist_id, artist_name in conn.execute(sql_code, args):
        print('Artist id : {}; Artist name: {}'.format(artist_id, artist_name))

    print('---')
    
    # Question 2
    print('Question 2: What are the different types of album?')
    print('4 album types in total:')
    
    # implement your solution to q2
    for a_type in conn.execute('SELECT DISTINCT album_type FROM album WHERE album_type IS NOT NULL'):
        print('Album type: {}'.format(a_type[0]))

    print('---')
    
    # Question 3
    print('Question 3: Which album (id and title) has the most listens?')
    
    # implement your solution to q3
    for album_id, album_title in conn.execute('SELECT id, album_title FROM album ORDER BY album_listens DESC LIMIT 1'):
        print('Album id : {}; Album title: {}'.format(album_id, album_title))

    print('---')
    
    # Question 4
    print('Question 4: How many artists have a wikipedia page?')
    
    # implement your solution to q4
    for row in conn.execute("SELECT COUNT(*) FROM artist WHERE artist_wikipedia_page IS NOT NULL"):
        print('{} artists have a wikipedia page'.format(row[0]))
    
    print('---')
    
    # Question 5
    print('Question 5: Which non-null language codes have 3 or more tracks?')
    print("9 language codes in total have 3 or more tracks, and they are:")
    
    # implement your solution to q5
    for language_code in conn.execute("SELECT track_language_code FROM track WHERE track_language_code IS NOT NULL GROUP BY track_language_code HAVING COUNT(*) >= 3 ORDER BY COUNT(*) DESC"):
        print(language_code[0])
    
    print('---')
    
    # Question 6
    print('Question 6: How many tracks are by artists known to be from the southern hemisphere (i.e., with latitude < 0)')
    
    # implement your solution to q6
    for row in conn.execute("SELECT COUNT(*) FROM track t LEFT JOIN artist a ON t.artist_id = a.id WHERE a.artist_latitude < 0"):
        print('{} tracks are by artists known to be from the southern hemisphere'.format(row[0]))
    
    print('---')
    
    # Question 7
    print("Question 7: How many albums are there where the album's title is identical to the artist's name?")
    for row in conn.execute("""SELECT COUNT(DISTINCT al.album_title)
                               FROM track t
                               INNER JOIN album al ON t.album_id = al.id
                               INNER JOIN artist ar ON t.artist_id = ar.id
                               WHERE al.album_title = ar.artist_name"""):
        print("{} albums are there where the album's title is identical to the artist's name".format(row[0]))
    
    # implement your solution to q7
    
    print('---')
    
    # Question 8
    print('Question 8: Which artist (including id and name) appears on the largest number of distinct albums?')
    
    # implement your solution to q8
    for artist_id, artist_name in conn.execute("""SELECT arid, arname
                                                    FROM
                                                    (SELECT al.id AS alid, ar.id AS arid, ar.artist_name AS arname
                                                    FROM track t
                                                    INNER JOIN album al ON t.album_id = al.id
                                                    INNER JOIN artist ar ON t.artist_id = ar.id
                                                    GROUP BY al.id)
                                                    GROUP BY arid
                                                    ORDER BY COUNT(alid) DESC
                                                    LIMIT 1"""):
        print("{} appears on the largest number of distinct albums with artist id: {}".format(artist_name, artist_id))
    
    print('---')
