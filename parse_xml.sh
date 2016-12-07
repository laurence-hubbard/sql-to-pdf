#! /bin/bash

SQL=/Users/LHubbard/Desktop/projects/graphviz/SQL
TEMP=/tmp/LHubbard/projects/graphviz
DOT=/Users/LHubbard/Desktop/projects/graphviz/DOT
mkdir -p $TEMP

# Local Mac version of sed is old
alias sed='gsed'

# Need to source a processing power script (something that can pull HDFS files & process all job types using functions only)
# Need a method of going through a flattened XML, splitting by workflow and find and replacing the variables
	# Need to get the XML of the coordinators (oozie jobs -jobtype coordinator to get list) and then need to match workflows to coordinators!!! :(

cat $1 | awk '{print}' ORS=' ' | sed 's/\t/ /g' | tr -s ' ' | sed 's/> </></g' | sed 's/>/>\n/g' | sed 's/</\n</g' | grep -v ^$ > $TEMP/xml_flattened.tmp

xml_crunch (){
CRUNCH=$1
CLOSE=$(echo $CRUNCH | sed 's/</<\//g')
JOBTYPE=$(echo $CRUNCH | sed 's/[<>]//g')
#grep -n "^$CRUNCH" $TEMP/xml_flattened.tmp
grep -n "^$CRUNCH" $TEMP/xml_flattened.tmp | cut -d':' -f1 > $TEMP/crunch.numbers

while read TOP; do
	BOTTOM=$(cat $TEMP/xml_flattened.tmp | tail -n +$TOP | grep -n "^$CLOSE" | head -1 | cut -d':' -f1)
	cat $TEMP/xml_flattened.tmp | tail -n +$TOP | head -$BOTTOM > $TEMP/the_job.tmp

	if [ $JOBTYPE == "distcp" ]; then
		echo distcp is going to be processed now
		cat $TEMP/the_job.tmp
	else
		echo Unknown JOBTYPE encountered: $JOBTYPE
	fi


done < $TEMP/crunch.numbers

}

xml_crunch "<distcp"

#xml_crunch "<fs"

echo $TEMP/xml_flattened.tmp
