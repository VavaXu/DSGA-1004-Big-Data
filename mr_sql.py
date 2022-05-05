#! /usr/bin/env python

from mrjob.job import MRJob

current_key = None

class MRMoviesByGenreCount(MRJob):
    """
    Find the distinct number of movies in each genre.
    """

    def mapper(self, _, line):
        name, genre = line.split(",")
        if genre != 'Horror':
            yield (genre, name)

    def reducer(self, genre, name):
        names = list(set(name))
        if len(names) >= 100:
            yield (genre, len(names))

# don't forget the '__name__' == '__main__' clause!
if __name__ == '__main__':
    MRMoviesByGenreCount.run()
