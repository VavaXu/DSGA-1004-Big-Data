#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
#   python lab1_part2.py music_small.db

# Ziwei Xu (Vanessa)
# NetID: zx657

import sys
import sqlite3
import timeit

# The database file should be given as the first argument on the command line
# Please do not hard code the database file!
db_file = sys.argv[1]


# The query to be optimized is given here
MY_QUERY = """SELECT artist.*, count(distinct track.id) as tracks
                FROM track INNER JOIN artist
                ON track.artist_id = artist.id
                GROUP BY artist_id
                HAVING tracks > 10"""

NUM_ITERATIONS = 100

def run_my_query(conn):
    for row in conn.execute(MY_QUERY):
        pass
    
# We connect to the database using
with sqlite3.connect(db_file) as conn:
    # We use a "cursor" to mark our place in the database.
    cursor = conn.cursor()

    # We could use multiple cursors to keep track of multiple
    # queries simultaneously.

    orig_time = timeit.repeat('run_my_query(conn)', globals=globals(), number=NUM_ITERATIONS)
    print("Before optimization:")

    print(f'Mean time: {sum(orig_time)/NUM_ITERATIONS:.3f} [seconds/query]')
    print(f'Best time: {min(orig_time)/NUM_ITERATIONS:.3f} [seconds/query]')

    # MAKE YOUR MODIFICATIONS TO THE DATABASE HERE
    # Create index on artist_id column in track table
    for row in conn.execute('CREATE INDEX ind ON track (artist_id)'):
        pass
    # result: Mean time: 0.023 [seconds/query]; Best time: 0.005 [seconds/query]

    new_time = timeit.repeat('run_my_query(conn)', globals=globals(), number=NUM_ITERATIONS)
    print("After optimization:")

    print(f'Mean time: {sum(new_time)/NUM_ITERATIONS:.3f} [seconds/query]')
    print(f'Best time: {min(new_time)/NUM_ITERATIONS:.3f} [seconds/query]')
