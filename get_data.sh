#!/bin/bash

cd data

# Download hourly climate data
wget -r -nH --cut-dirs=6 -q --show-progress ftp://ftp-cdc.dwd.de//pub/CDC/observations_germany/climate/hourly/

# Download station metadata
# ftp://ftp-cdc.dwd.de/pub/CDC/help/EB_Stundenwerte_Beschreibung_Stationen.txt

# Unzip data
find . -name "*.zip" | while read filename; do unzip -q -o -d "`dirname "$filename"`" "$filename"; done;

# Delete zip, html and pdf files
find . -type f -name '*.zip' -delete
find . -type f -name '*.html' -delete
find . -type f -name '*.pdf' -delete

# Convert text files from ISO-8859-1 to UTF-8
FILELIST=$(find . -type f -name "*.txt")

for file in $FILELIST
do
    iconv --from-code='ISO-8859-1' --to-code='UTF-8' "$file" | sponge "$file"
done

