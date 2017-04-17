import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime
import re


def check_from_date(x):
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


def check_from_time(x):
    time = x.split(':')
    if len(time) != 3:
        return 'NULL'
    try:
        H = int(time[0])
        M = int(time[1])
        S = int(time[2])
    except Exception as e:
        return 'NULL'
    if (H not in range(0, 24)) or (M not in range(0, 60)) or (S not in range(0, 60)):
        return 'INVALID'
    else:
        return 'VALID'


def check_to_date(x, y, validy):
    date = check_from_date(x)
    if date != 'VALID':
        return date
    elif validy == 'VALID':
        dateformatx = datetime.strptime(x, '%m/%d/%Y')
        dateformaty = datetime.strptime(y, '%m/%d/%Y')
        if dateformatx >= dateformaty:
            return 'VALID'
        else:
            return 'INVALID'
    else:
        return 'VALID'


def check_to_time(x):
    return check_from_time(x)


def check_report_date(x, y, validy):
    return check_to_date(x, y, validy)


def check_key_code(x):
    if len(x) != 3:
        return 'NULL'
    else:
        pattern = re.compile(r"\d{3}")
    if pattern.match(x) is None:
        return 'NULL'
    else:
        return 'VALID'


def check_offense_description(x):
    if x is None or x in ['NA', 'N/A', 'NONE', 'NOT SPECIFIED', 'NOT-SPECIFIED', 'NOT AVAILABLE', 'UNAVAILABLE']:
        return 'NULL'
    pattern = re.compile(r".*[A-Z]+")
    if pattern.match(x) is None:
        return 'NULL'
    else:
        return 'VALID'


def check_pd_code(x):
    return check_key_code(x)


def check_pd_description(x):
    return check_offense_description(x)


def check_crime_completed(x):
    if x is None:
        return 'NULL'
    elif x == 'COMPLETED' or x == 'ATTEMPTED':
        return 'VALID'
    else:
        return 'NULL'


def check_offense_level(x):
    if x is None:
        return 'NULL'
    elif x in ['MISDEMEANOR', 'FELONY', 'VIOLATION']:
        return 'VALID'
    else:
        return 'NULL'


def check_jurisdiction(x):
    return check_offense_description(x)


def check_borough(x):
    if x is None:
        return 'NULL'
    elif x in ['MANHATTAN', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND']:
        return 'VALID'
    else:
        return 'NULL'


def check_precinct(x):
    if x is None:
        return 'NULL'
    try:
        p = int(x)
    except Exception as e:
        return 'INVALID'
    if p in range(1, 124):
        return 'VALID'
    else:
        return 'NULL'


def check_specific_location(x):
    if x is None:
        return 'NULL'
    if x in ['INSIDE', 'FRONT OF', 'OPPOSITE OF', 'REAR OF', 'OUTSIDE']:
        return 'VALID'
    else:
        return 'NULL'


def check_premises(x):
    return check_offense_description(x)


def check_park(x):
    return check_offense_description(x)


def check_HD(x):
    return check_offense_description(x)


def check_xco(x):
    if x is None:
        return 'NULL'
    try:
        xc = int(x)
    except Exception as e:
        return 'NULL'
    if xc in range (909900, 1067601):
        return 'VALID'
    else:
        return 'INVALID'


def check_yco(y):
    if y is None:
        return 'NULL'
    try:
        yc = int(y)
    except Exception as e:
        return 'NULL'
    if yc in range (117500, 275001):
        return 'VALID'
    else:
        return 'INVALID'


def check_latitude(x):
    if x is None:
        return 'NULL'
    try:
        lat = float(x)
    except Exception as e:
        return 'NULL'
    if 40.0 < lat < 41.0:
        return 'VALID'
    else:
        return 'INVALID'


def check_longitude(x):
    if x is None:
        return 'NULL'
    try:
        lon = float(x)
    except Exception as e:
        return 'NULL'
    if -73.0 < lon < -75.0:
        return 'VALID'
    else:
        return 'INVALID'

sc = SparkContext()
csvfile = sc.textFile(sys.argv[1], 1)
crimedata = csvfile.mapPartitions(lambda x: reader(x))
fromdate = crimedata.map(lambda x: (x[0], (x[1], "DATE", "Complaint from date", "date", check_from_date(x[1]))))
fromtime = crimedata.map(lambda x: (x[0], (x[2], "TIME", "Complaint from time", "time", check_from_time(x[2]))))
todate = crimedata.map(lambda x: (x[0], x[3]))
todate = fromdate.join(todate).map(
    lambda x: (x[0], ("DATE", "Complaint to date", "date", check_to_date(x[1][1], x[1][0][0], x[1][0][4]))))
totime = crimedata.map(lambda x: (x[4], "TIME", "Complaint to time", "time", check_to_time(x[4])))
reportdate = crimedata.map(lambda x: (x[0], x[5]))
reportdate = fromdate.join(reportdate).map(
    lambda x: (x[0], ("DATE", "Complaint report date", "date", check_report_date(x[1][1], x[1][0][0], x[1][0][4]))))
offensecode = crimedata.map(
    lambda x: (x[0], (x[6], "INTEGER", "3-digit offense code", "3-digit code", check_key_code(x[6]))))
offensedescription = crimedata.map(
    lambda x: (x[0], (x[7], "STRING", "Offense Description", "text", check_offense_description(x[7]))))
internalcode = crimedata.map(
    lambda x: (x[0], (x[8], "INTEGER", "3-digit PD internal offense code", "3-digit code", check_pd_code(x[8]))))
internaldescription = crimedata.map(
    lambda x: (x[0], (x[9], "STRING", "PD Internal Offense Description", "text", check_pd_description(x[9]))))
crimecompleted = crimedata.map(lambda x: (x[0],(x[10], "STRING", "Crime completed status", "label", check_crime_completed(x[10]))))
offenselevel = crimedata.map(lambda x: (x[0],(x[11], "STRING", "Level of offense", "label", check_offense_level(x[11]))))
jurisdiction = crimedata.map(lambda x: (x[0],(x[12], "STRING", "Jurisdiction responsible", "string", check_jurisdiction(x[12]))))
borough = crimedata.map(lambda x: (x[0],(x[13], "STRING", "Borough of incident", "string", check_borough(x[13]))))
pricinct = crimedata.map(lambda x: (x[0],(x[14], "INTEGER", "Precinct number", "3-digit code", check_precinct(x[14]))))
specificlocation = crimedata.map(lambda x: (x[0],(x[15], "STRING", "Specific Location of incident", "label", check_specific_location(x[15]))))
premises = crimedata.map(lambda x: (x[0],(x[16], "STRING", "Premises of incident", "text", check_premises(x[16]))))
park = crimedata.map(lambda x: (x[0],(x[17], "STRING", "Park or public grounds", "text", check_park(x[17]))))
housingdevelopment = crimedata.map(lambda x: (x[0],(x[18], "STRING", "NYCHA housing development of occurence", "text", check_HD(x[18]))))
xcoordinate = crimedata.map(lambda x: (x[0],(x[19], "INTEGER", "X-coordinate for New York State Plane Coordinate System", "6-7 digit integer", check_xco(x[19]))))
ycoordinate = crimedata.map(lambda x: (x[0],(x[20], "INTEGER", "Y-coordinate for New York State Plane Coordinate System", "6 digit integer", check_yco(x[20]))))
latitude = crimedata.map(lambda x: (x[0],(x[21], "FLOATING POINT", "Latitude coordinate", "latitude", check_latitude(x[21]))))
longitude = crimedata.map(lambda x: (x[0],(x[22], "FLOATING POINT", "Longitude coordinate", "longitude", check_longitude(x[22]))))
fromdate.map(lambda x : x[1]).saveAsTextFile("fromdate.out")
fromdate.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
fromtime.map(lambda x : x[1]).saveAsTextFile("fromtime.out")
fromtime.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
todate.map(lambda x : x[1]).saveAsTextFile("todate.out")
todate.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
totime.map(lambda x : x[1]).saveAsTextFile("totime.out")
totime.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
reportdate.map(lambda x : x[1]).saveAsTextFile("reportdate.out")
reportdate.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
offensecode.map(lambda x : x[1]).saveAsTextFile("offensecode.out")
offensecode.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
offensedescription.map(lambda x : x[1]).saveAsTextFile("offensedescription.out")
offensedescription.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
internalcode.map(lambda x : x[1]).saveAsTextFile("internalcode.out")
internalcode.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
internaldescription.map(lambda x : x[1]).saveAsTextFile("internaldescription.out")
internaldescription.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
crimecompleted.map(lambda x : x[1]).saveAsTextFile("crimecompleted.out")
crimecompleted.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
offenselevel.map(lambda x : x[1]).saveAsTextFile("offenselevel.out")
offenselevel.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
jurisdiction.map(lambda x : x[1]).saveAsTextFile("jurisdiction.out")
jurisdiction.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
borough.map(lambda x : x[1]).saveAsTextFile("borough.out")
borough.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
pricinct.map(lambda x : x[1]).saveAsTextFile("pricinct.out")
pricinct.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
specificlocation.map(lambda x : x[1]).saveAsTextFile("specificlocation.out")
specificlocation.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
premises.map(lambda x : x[1]).saveAsTextFile("premises.out")
premises.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
park.map(lambda x : x[1]).saveAsTextFile("park.out")
park.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
housingdevelopment.map(lambda x : x[1]).saveAsTextFile("housingdevelopment.out")
housingdevelopment.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
xcoordinate.map(lambda x : x[1]).saveAsTextFile("xcoordinate.out")
xcoordinate.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
ycoordinate.map(lambda x : x[1]).saveAsTextFile("ycoordinate.out")
ycoordinate.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
latitude.map(lambda x : x[1]).saveAsTextFile("latitude.out")
latitude.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()
longitude.map(lambda x : x[1]).saveAsTextFile("longitude.out")
longitude.map(lambda x: (x[1][4],1)).reduceByKey(lambda x,y : x+y).collect()

