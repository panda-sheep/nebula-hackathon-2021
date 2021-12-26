#! /bin/bash

> /data/pandasheep/hackathon/timeResult

# The first 4 lines are the result of use space, and need to be deleted
sed -i '1,5d' /data/pandasheep/hackathon/result

sed -n '/time spent/w /data/pandasheep/hackathon/timeResult' /data/pandasheep/hackathon/result

# Empty set (time spent 4556/4845 us)
cat /data/pandasheep/hackathon/timeResult | awk '{ print $6}' | awk -F '/' '{sum += $1}; END {print sum}'

