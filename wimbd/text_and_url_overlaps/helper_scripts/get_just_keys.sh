#!/bin/bash
# This script is used to get the all keys from the output of the
# either the text or url overlap script.

sed 's/^[ \t]*//' | cut -d ' ' -f2- 

# this works by removing the leading spaces and tabs, then printing the
# lines that have a number greater than 1, then cutting the number off
# the front of the line, which leaves the duplicate keys by themselves.

