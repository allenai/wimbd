#!/bin/bash
# This script is used to get the duplicate keys from the output of the
# either the text or url overlap script.

pv -N 'Finding dups' | sed 's/^[ \t]*//' |  awk -F ' ' '{if($1>1)print}' | cut -d ' ' -f2- 

# this works by removing the leading spaces and tabs, then printing the
# lines that have a number greater than 1, then cutting the number off
# the front of the line, which leaves the duplicate keys by themselves.

