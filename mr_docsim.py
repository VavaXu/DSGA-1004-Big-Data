#! /usr/bin/env python

import re
import string
import pathlib

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

WORD_RE = re.compile(r"[\S]+")

class MRDocSim(MRJob):
    def mapper_get_words(self, _, line):
        # This part extracts the name of the current document being processed
        current_file = jobconf_from_env("mapreduce.map.input.file")
        # Use this doc_name as the identifier of the document
        doc_name = pathlib.Path(current_file).stem
        # storing all doc_name into doclist 
        word_true = False
        for word in WORD_RE.findall(line):
            # strip any punctuation
            word = word.strip(string.punctuation)
            # enforce lowercase
            word = word.lower()
            word_true = True
            yield ((doc_name, word), 1)
        word_f = "__"
        yield ((doc_name, word_f), 0)

    def combiner_doc_word_count(self, doc_word, count):
        counts = sum(count)
        yield (docword, counts)

    def reducer_group_by_word(self, doc_word, counts):
        word_counts = list(counts)
        word = doc_word[1]
        doc = doc_word[0]
        yield (word, (doc, word_counts))

    def reducer_worddoccount(self, word, doc_count):
        doc_counts = list(doc_count)
        yield (word, doc_counts)

    def mapper_pair_min(self, word, doc_counts):
        length = len(doc_counts)
        for i in range(length):
            for j in range(length):
                document1 = doc_counts[i][0]
                document2 = doc_counts[j][0]
                value1 = doc_counts[i][1]
                value2 = doc_counts[j][1]
                if value1 <= value2:
                    min_value = value1
                else:
                    min_value = value2
                yield ((document1, document2), min_value[0])

    def reducer_final_sim(self, pair, mins):
        sim = sum(mins)
        yield (pair, sim)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_words,
                combiner=self.combiner_doc_word_count,
                reducer=self.reducer_group_by_word),
            MRStep(
                reducer=self.reducer_worddoccount),

            MRStep(
                mapper=self.mapper_pair_min,
                reducer=self.reducer_final_sim),
        ]

if __name__ == "__main__":
    MRDocSim.run()
