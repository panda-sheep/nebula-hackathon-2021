#! /bin/bash
head -10000 /data/pandasheep/graph500/vertex.csv > /data/pandasheep/hackathon/data.csv

echo "use nb;" > /data/pandasheep/hackathon/hackathon.sql

# case one: GO FROM "3235527" OVER FOLLOW yield FOLLOW._dst;
cat /data/pandasheep/hackathon/data.csv | awk '{ print "GO FROM \""$1"\" OVER FOLLOW yield FOLLOW._dst;" >> "/data/pandasheep/hackathon/hackathon.sql"};'

# case two: GO FROM "3235527" OVER FOLLOW yield FOLLOW._dst as id |yield count($-.id);
cat /data/pandasheep/hackathon/data.csv | awk '{ print "GO FROM \""$1"\" OVER FOLLOW yield FOLLOW._dst as id \| yield count(\$\-\.id);" >> "/data/pandasheep/hackathon/hackathon.sql"};'

#case three: fetch outdegree("3235527")
cat /data/pandasheep/hackathon/data.csv | awk '{ print "fetch outdegree(\""$1"\", FOLLOW);" >> "/data/pandasheep/hackathon/hackathon.sql"};'
