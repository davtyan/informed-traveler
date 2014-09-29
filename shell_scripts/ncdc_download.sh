#!/bin/bash

clear

echo "Start downloading NCDC Weather data (1987 - 2014)"

echo

echo "The Data will be saved under /home/ubuntu/Data/NCDC"

#using curl to download the data

cd /home/ubuntu/Data/NCDC && { curl -O http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/[1987-2014].csv.gz ; cd -; }



