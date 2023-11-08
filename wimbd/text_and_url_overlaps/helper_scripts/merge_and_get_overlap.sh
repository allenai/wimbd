#!/bin/bash
# merge already sorted files, and find the hashes that appear more than once, these are the overlaps
sort -m  $1 $2 | uniq -c | sed 's/^[ \t]*//' |  awk -F ' ' '{if($1>1)print}' | cut -d ' ' -f2- 