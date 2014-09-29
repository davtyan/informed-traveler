#!/bin/bash

if [$1 == ""]
then
    echo "Provide a path to the directory containing zipped files!"
else
    if [ -d $1 ]; then
	ZFILES=`ls $1 | grep .zip`

	for file in $ZFILES
	do
	    unzip -C $1/$file "*.csv" -d $1/unzipped
	done
    else "This is not a directory!"
    fi
fi
