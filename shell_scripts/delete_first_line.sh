#!/bin/bash

if [$1 == ""]
then
    echo "Provide a path to the directory containing the .csv files."
else
    if [ -d $1 ]; then
	echo "This is a directory"
	CSVFILES=`ls $1 | grep .csv`

	for file in $CSVFILES
	do
	    sed -i '1d' $1/$file
	done
    fi
fi
