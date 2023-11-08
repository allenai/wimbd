#!/bin/bash
sed 's/^[ \t]*//' $1 | cut -d ' ' -f2- 