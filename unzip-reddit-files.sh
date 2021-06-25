#!/bin/bash

# This script unzips the reddit files in the *zipped* folder into the *unzipped* folder
# Using a script because it's much faster (and easier) than doing this in Python

for file in ./Reddit_Posts/zipped/*.bz2
do
	echo "Unzipping $file"
	bunzip2 $file
done
