from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import os
from csv import reader

#Count the total felony complaint descriptions by year
#output: key = year, value = count


if __name__ == "__main__":
    def filter_date(x):
        try:
            x.split('/')[2]
            return True
        except Exception as e:
            return False

    def filter_felony(x):
        if 'FELONY' in x:
            return True
        else:
            return False


    if len(sys.argv) != 2:
        print("Usage: felony_count_by_desc_lvl2.py <file1> ", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    data= sc.textFile(sys.argv[1], 1)
    rdd = data.mapPartitions(lambda x: reader(x))
    mapping = rdd.map(lambda line: (line[5], line[11], line[7], line[9])).filter(lambda x: filter_date(x[0])).filter(lambda x: filter_felony(x[1])).map(lambda x: (str(x[2])+'_'+str(x[3]), 1))
    joined = mapping.reduceByKey(lambda x,y: x+y).sortByKey(ascending=True).saveAsTextFile("felony_count_by_desc_lvl2.out")
