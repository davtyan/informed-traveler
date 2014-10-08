#!/bin/bash

clear

echo "Start downloading On-Time Performance Data from Rita (January 1987 - July 2014)"

echo

echo "The Data will be saved under /home/ubuntu/seda/informed-traveler/Data"

#using curl to download the data

cd /home/ubuntu/seda/informed-traveler/Data && { curl -O http://www.transtats.bts.gov/Download/On_Time_On_Time_Performance_[1987-2013]_[1-12].zip ; cd -; }

#2014 needs to be processed separately since the data available only up until July

cd /home/ubuntu/seda/informed-traveler/Data && { curl -O http://www.transtats.bts.gov/Download/On_Time_On_Time_Performance_2014_[1-7].zip ; cd -; }

