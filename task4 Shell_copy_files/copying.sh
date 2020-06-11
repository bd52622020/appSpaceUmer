#!/bin/bash
find "$1" -name "*.html" -exec cp {} "$2" \;
declare -i counter=1
find "$2"  -name "*html" -print0 | while IFS= read -r -d '' path
do
    base=$(basename --suffix=".html"  "$path")
    new_path="${2%/}"/"$base"$counter".html"
    cp "$path" "$new_path"
    counter=$(( $counter ))
done
