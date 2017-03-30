from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import os
from csv import reader

#Count the total number of complaints types per year from 2006 - 2016
#output: key = year_type, value = count


if __name__ == "__main__":
    def filter_date(x):
        try:
            x.split('/')[2]
            return True
        except Exception as e:
            return False


    if len(sys.argv) != 2:
        print("Usage: yearly_complaint_type_count.py <file1> ", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    data= sc.textFile(sys.argv[1], 1)
    rdd = data.mapPartitions(lambda x: reader(x))
    mapping = rdd.map(lambda line: (line[5], line[11])).filter(lambda x: filter_date(x[0])).map(lambda x: (str(x[0].split('/')[2])+'_'+str(x[1]), 1))
    joined = mapping.reduceByKey(lambda x,y: x+y).sortByKey(ascending=True).saveAsTextFile("yearly_complaint_type_count.out")

