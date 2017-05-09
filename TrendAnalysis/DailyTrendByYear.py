from csv import reader
import sys
from pyspark import SparkContext


def check_date(x):
    date = x.split('/')
    if len(date) != 3:
        return 'NULL'
    try:
        m = int(date[0])
        d = int(date[1])
        Y = int(date[2])
    except Exception as e:
        return 'NULL'
    if (m not in range(1, 13)) or (d not in range(1, 32)) or (Y not in range(1900, 2016)):
        return 'INVALID'
    else:
        return 'VALID'


def getdate(x, y, z):
    if check_date(x) == 'VALID':
        return x
    else:
        if check_date(y) == 'VALID':
            return y
        else:
            if check_date(z) == 'VALID':
                return z
            else:
                return 'NULL'


def getyear(x):
    if x != 'NULL':
        return x.split('/')[2]

csvfile = sc.textFile(sys.argv[1], 1)
crimedata = csvfile.mapPartitions(lambda x: reader(x))
crimedata.map(lambda x: getdate(x[1], x[3], x[5])).filter(lambda x: getyear(x) == sys.argv[2]).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).saveAsTextFile(sys.argv[2]+".out")