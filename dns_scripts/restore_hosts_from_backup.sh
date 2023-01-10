#!/bin/bash

HOSTS_BACKUP=$(cat backup/hosts)

BACKUPS=$(find backup -name "hosts*" 2> /dev/null | sort -n)

echo "Choose backup to restore (enter number):"

i=0

files=()

for line in $BACKUPS;
do
    i=$((i+1))

    files[$i]=$line

    echo "$i: $line"
done

read -r choice

FILE=${files[$choice]}

echo "$FILE"
FILE_CONTENT=$(cat "$FILE")

echo "The following content will be written to file /etc/hosts:"
echo "$FILE_CONTENT"

echo "Do you wish to continue? (enter number)"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) break;;
        No ) exit;;
    esac
done

echo "Restoring /etc/hosts/ from $FILE"

echo "$FILE_CONTENT" > /etc/hosts
