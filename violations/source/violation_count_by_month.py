from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import os
from csv import reader

#Count the total violation complaints by month from 2006 - 2016
#output: key = numeric_month, value = count


if __name__ == "__main__":
    def filter_date(x):
        try:
            x.split('/')[0]
            return True
        except Exception as e:
            return False

    def filter_violation(x):
        if 'VIOLATION' in x:
            return True
        else:
            return False


    if len(sys.argv) != 2:
        print("Usage: violation_count_by_month.py <file1> ", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    data= sc.textFile(sys.argv[1], 1)
    rdd = data.mapPartitions(lambda x: reader(x))
    mapping = rdd.map(lambda line: (line[5], line[11])).filter(lambda x: filter_date(x[0])).filter(lambda x: filter_violation(x[1])).map(lambda x: (str(x[0].split('/')[0]), 1))
    joined = mapping.reduceByKey(lambda x,y: x+y).sortByKey(ascending=True).saveAsTextFile("violation_count_by_month.out")
