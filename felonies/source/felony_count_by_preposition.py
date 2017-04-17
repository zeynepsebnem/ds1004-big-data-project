from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import os
from csv import reader

#Count the total felony complaints by the preposition describing where they were reported
#output: key = preposition, value = count


if __name__ == "__main__":

    def filter_felony(x):
        if 'FELONY' in x:
            return True
        else:
            return False


    if len(sys.argv) != 2:
        print("Usage: felony_count_by_preposition.py <file1> ", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    data= sc.textFile(sys.argv[1], 1)
    rdd = data.mapPartitions(lambda x: reader(x))
    mapping = rdd.map(lambda line: (line[11], line[15])).filter(lambda x: filter_felony(x[0])).map(lambda x: (x[1], 1))
    joined = mapping.reduceByKey(lambda x,y: x+y).sortByKey(ascending=True).saveAsTextFile("felony_count_by_preposition.out")
