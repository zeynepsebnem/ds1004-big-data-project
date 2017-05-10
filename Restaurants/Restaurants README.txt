The count_restaurants_by_boro.py and count_restaurants_by_boro_year.py scripts output the counts of new restaurants by borough and by borough and year (respectively). We count new restaurants by counting the number of pre-permit inspections found in the DOHMH restaurant insepction dataset. The Pre-Permit_Restaurant_Inspections.csv file is a filtered version of that dataset (described in more detail below). The new_restaurants_by_boro.out and new_restaurants_by_boro_year.out are the outputs of the PySpark scripts.

Before running the scripts using spark-submit, please run the following commands:
module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

The file passed as an argument when using spar-submit should be the restaurant inspection dataset
available from https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j/data
but filtered using the NYC OpenData interactive filtering tool to only include the following inspection types:
Pre-permit (Operational) / Initial Inspection
Pre-permit (Non-operational) / Initial Inspection

The filtered view is available for download here:
https://data.cityofnewyork.us/Health/Pre-Permit-Restaurant-Inspections/jzz4-5r78/data

The Correlation with Restaurants.ipynb notebook should be run in Jupyter notebook. It generates visualizations and OLS linear regression modeling results for the number of new restaurants vs. the counts of felonies, misdemeanors, and violations year-by-year for each borough.