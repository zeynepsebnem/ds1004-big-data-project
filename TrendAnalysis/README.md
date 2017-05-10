DailyTrendByYear.py
* Provides a mapping of days of year and number of incidents on that day.
* Run this code with spark-submit shell script with three parameters as follows:

Usage: spark-submit DailyTrendByYear.py <path to NYPD crime data> <four-digit year>

* Store the merged outputs of each year in the folder “DailyTrendByYearOut”.

DaysByYearPlot.ipynb
* Outputs the plot of daily trend of number of incidents for the selected year.
* Keep this file in the same parent directory as “DailyTrendByYearOut”. Create another folder “plots” to store the outputs.
* Run this code with Jupyter Notebook, varying the values of ‘year’ variable between 2005-2015.

SpikesInYear.ipynb
* Provides the days with number of incidents falling outside 3 standard deviations from mean.
* Keep this file in the same directory as above.
* Run the code with Jupyter Notebook, varying the values of ‘year’ variable between 2005-2015.