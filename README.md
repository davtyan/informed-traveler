Informed Traveler
=================

Pick an airline wisely, be informed!
====================================

**Introduction**
Informed traveler is a web application that can be used for fast and user friendly querying of *Airline On-Time Performance* Data available from the *RITA (Research and Innovative Technology and Administration)* web site [RITA](http://www.rita.dot.gov). The data is updated on a quarterly basis and it goes back to October 1987. The historic data amounts to 65FB.

To automatically download the data I used [rita_data_download.sh](../shell_scripts/rita_data_download.sh). I then did some pre-processing using shell scripts (can be found under [shell_scripts](../shell_scripts)) prior to putting the data on to HDFS.

Here is the data pipeline that I used for my project:

![alt text](images/data-pipeline.jpg)
