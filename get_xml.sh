#! /bin/bash

VERBOSE=false

TEMP=/home/likewise-open/INSURANCE/l_hubbard/graphviz/TMP
OUT=/home/likewise-open/INSURANCE/l_hubbard/graphviz/OUT

touch $TEMP/paths $TEMP/files
rm $TEMP/paths $TEMP/files

touch $TEMP/sql.all
rm $TEMP/sql.all

oozie jobs -oozie http://172.29.0.243:11000/oozie/ | tail -n +2 | grep -v ^- | cut -d' ' -f1 > $TEMP/jobs
#echo 0000335-160425115820002-oozie-oozi-W > $TEMP/jobs
#head -1 $TEMP/jobs > temp
#cat temp > $TEMP/jobs

# CustomerProfile Workflows
echo "0000860-160425115820002-oozie-oozi-W
0000863-160425115820002-oozie-oozi-W
0000867-160425115820002-oozie-oozi-W
0000871-160425115820002-oozie-oozi-W
0000872-160425115820002-oozie-oozi-W
0000873-160425115820002-oozie-oozi-W" > $TEMP/jobs

# All Workflows
#oozie jobs -oozie http://172.29.0.243:11000/oozie/ | tail -n +2 | grep -v ^- | cut -d' ' -f1 > $TEMP/jobs
#oozie jobs -oozie http://172.29.0.243:11000/oozie/ -len 10000 | tail -n +2 | egrep "2016-06-28|2016-06-27" | grep -v ^- | cut -d' ' -f1 > $TEMP/jobs
oozie jobs -oozie http://172.29.0.243:11000/oozie/ -len 10000 | tail -n +2 | grep -v ^- | cut -d' ' -f1 > $TEMP/jobs

#echo "0000859-160425115820002-oozie-oozi-W" > $TEMP/jobs

# CoreDataModel coord
#echo "0004837-151229141259471-oozie-oozi-W" > $TEMP/jobs

#app-path

xml_crunch (){
CRUNCH=$1
XML_FILE=$2
CLOSE=$(echo $CRUNCH | sed 's/</<\//g')
JOBTYPE=$(echo $CRUNCH | sed 's/[<>]//g')
#grep -n "^$CRUNCH" $TEMP/xml_flattened.tmp
grep -n "^$CRUNCH" $XML_FILE | cut -d':' -f1 > $TEMP/crunch.numbers

while read TOP; do
        BOTTOM=$(cat $TEMP/xml_flattened.tmp | tail -n +$TOP | grep -n "^$CLOSE" | head -1 | cut -d':' -f1)
        cat $XML_FILE | tail -n +$TOP | head -$BOTTOM > $TEMP/the_job.tmp

        if [ $JOBTYPE == "distcp" ]; then
                echo distcp is going to be processed now
                cat $TEMP/the_job.tmp
        else
                echo Unknown JOBTYPE encountered: $JOBTYPE
        fi
done < $TEMP/crunch.numbers

}

populate_property_variables (){
XML=$1
CONF=$2

echo running populate_property_variables for XML $XML and CONF $CONF

grep -o "\${[^{}]*}" $XML > $TEMP/xml.variables
cat $CONF | awk '{print}' ORS=' '

}

get_sql_files_from_xml (){
XML=$1
SQL=$2

cat $XML | awk '{print}' ORS=' ' | grep -o "<script>[^<>]*</script>" | sed 's/<[^<>]*>//g' | egrep -i "\.q$" > $SQL
$VERBOSE && echo $XML
$VERBOSE && cat $XML | awk '{print}' ORS=' ' | grep -o "<script>[^<>]*</script>" | sed 's/<[^<>]*>//g' | egrep -i "\.q$"
}


parse_xml (){
while read JOB; do
 #       oozie job -oozie http://172.29.0.243:11000/oozie/ -definition $JOB > $TEMP/xml
#	oozie job -oozie http://172.29.0.243:11000/oozie/ -configcontent $JOB > $TEMP/conf
	cat $TEMP/xml | awk '{print}' ORS=' ' | grep -o "<app-path>[^<>]*</app-path>" | sed 's/<[^<>]*>//g' | sed 's/^${[^{}]*}//g' > $TEMP/subworkflows
	rm -f $TEMP/xml.subs
	touch $TEMP/xml.subs
	while read SUB; do
		hadoop fs -text $SUB/workflow.xml >> $TEMP/xml.subs
	done < $TEMP/subworkflows
	cat $TEMP/xml.subs| awk '{print}' ORS=' ' | grep -o "<app-path>[^<>]*</app-path>" | sed 's/<[^<>]*>//g' | sed 's/^${[^{}]*}//g' > $TEMP/subworkflows.sub
	rm -f $TEMP/xml.subs.subs
	touch $TEMP/xml.subs.subs
	while read SUB; do
                hadoop fs -text $SUB/workflow.xml >> $TEMP/xml.subs.subs
        done < $TEMP/subworkflows.sub
	cat $TEMP/xml.subs $TEMP/xml.subs.subs >> $TEMP/xml
#	populate_property_variables $TEMP/xml $TEMP/conf

#        cat $TEMP/xml | awk '{print}' ORS=' ' | grep -o "<script>[^<>]*</script>" | sed 's/<[^<>]*>//g' | egrep -i "\.q$" > $TEMP/sql
	get_sql_files_from_xml $TEMP/xml $TEMP/sql

        cat $TEMP/sql | sed 's/&amp;/_/g' >> $TEMP/sql.all
done < $TEMP/jobs
}


parse_xml

unscheduled_oozie_workflow() {
hadoop fs -text /user/hue/oozie/workspaces/_admin_-oozie-53435-1452593427.5/workflow.xml > $TEMP/xml
$VERBOSE && hadoop fs -text /user/hue/oozie/workspaces/_admin_-oozie-53435-1452593427.5/workflow.xml
get_sql_files_from_xml $TEMP/xml $TEMP/sql
cat $TEMP/sql | sed 's/&amp;/_/g' >> $TEMP/sql.all
}

#unscheduled_oozie_workflow

cat $TEMP/sql.all | sort | uniq > $TEMP/sql.all.sorted

while read SQL; do
#        echo hadoop fs -get $SQL $OUT/$(basename $SQL)
        hadoop fs -get $SQL $OUT/$(basename $SQL) 2> /dev/null
        echo $SQL >> $TEMP/paths
        echo $(basename $SQL) >> $TEMP/files
	cat $OUT/$(basename $SQL)
	echo ";"
done < $TEMP/sql.all.sorted

#cat $TEMP/files
