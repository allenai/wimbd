#!/bin/bash

# find the url location
if [ "$(zcat $1 | head -n 1| jq -r '.url')" != null ]; then
    url_location=".url"
else
    if [ "$(zcat $1 | head -n 1| jq -r '.metadata.url')"  != null ]; then
        url_location=".metadata.url"
    else
        echo $1
        echo "url location not found"
        exit 1
    fi
fi

parallel --line-buffer "gzip -dc {} | jq -r $url_location " ::: $@ | sort | uniq -c