This directory contains a shapefile and related files for New York neighborhoods and a Jupyter notebook. The notebook uses geopandas to play with the NYC neighborhoods shapefile, which is available along with the related files at https://www1.nyc.gov/site/planning/data-maps/open-data/bytes-archive.page. Please download those files first and save them in the same directory as this notebook. The notebook also maps crimes to neighborhoods for the subset of the data belonging to Brooklyn and will save several CSVs into the same directory as the notebook.

Before running the notebook, please install the geopandas package. The easiest way to do so is to follow these steps:
1. Download and install Anaconda from https://www.continuum.io/downloads
2. Installing geopandas from conda-forge by running
    conda install -c conda-forge geopandas

Please also download and unzip the three zipped folders that are not labeled "with_hood" available at https://github.com/zeynepsebnem/ds1004-big-data-project/tree/master/data/bk_slice. You will need to specify the path to those downloaded files in the notebook, if you did not download them into this directory.