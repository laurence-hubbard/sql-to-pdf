#! /bin/bash

# Variables to modify
VERBOSE=false
TEST_MODE=false
PROPERTY_TEST_MODE=false
DEV_MODE=false

# Set variables
TEMP=/home/likewise-open/INSURANCE/l_hubbard/graphviz/TMP
OUT=/home/likewise-open/INSURANCE/l_hubbard/graphviz/OUT

# Functions

produce_workflow_list (){
	WORKFLOW_LIST=$1

	$VERBOSE && echo produce_workflow_list $WORKFLOW_LIST
	oozie jobs -oozie http://172.29.0.243:11000/oozie/ -len 1000 | tail -n +2 | grep -v ^- | cut -d' ' -f1 > $WORKFLOW_LIST
	$TEST_MODE && head -10 $WORKFLOW_LIST > $TEMP/tmp && cat $TEMP/tmp > $WORKFLOW_LIST
	
}

append_sub_workflows (){
	MAJOR_XML=$1
	LOOP_INT=$2
	MINOR_XML=$TEMP/subworkflow_check_$LOOP_INT
	MINOR_HDFS_LIST=$TEMP/hdfs_list_$LOOP_INT

	$VERBOSE && echo append_sub_workflows $MAJOR_XML $LOOP_INT

	LOOP_INT=$((LOOP_INT+1))

	# Looking for subworkflows in major XML
	cat $MAJOR_XML | awk '{print}' ORS=' ' | grep -o "<app-path>[^<>]*</app-path>" | sed 's/<[^<>]*>//g' | sed 's/^${[^{}]*}//g' > $MINOR_HDFS_LIST
	[ $(cat $MINOR_HDFS_LIST | wc -l) -gt 0 ] && SUBWORKFLOWS=true || SUBWORKFLOWS=false
	if $SUBWORKFLOWS; then
	$VERBOSE && echo subworkflow found in $MAJOR_XML
	rm -f $MINOR_XML
	while read HDFS_WORKFLOW; do
		$DEV_MODE && hadoop fs -ls $HDFS_WORKFLOW/workflow.xml
		$DEV_MODE && hadoop fs -text $HDFS_WORKFLOW/workflow.xml
		hadoop fs -text $HDFS_WORKFLOW/workflow.xml >> $MINOR_XML
	done < $MINOR_HDFS_LIST
	append_sub_workflows $MINOR_XML $LOOP_INT

	# Referring to $1 and $2 as self-calling function doesn't handle variable names correctly
	$VERBOSE && echo appending sub workflow xmls $TEMP/subworkflow_check_$2 $1
	cat $TEMP/subworkflow_check_$2 >> $1

	fi
}

get_workflow_def (){
	WORKFLOW=$1
	WORKFLOW_DEF=$2

	$VERBOSE && echo get_workflow_def $WORKFLOW $WORKFLOW_DEF
	oozie job -oozie http://172.29.0.243:11000/oozie/ -definition $WORKFLOW > $WORKFLOW_DEF

	#[ $(cat $WORKFLOW_DEF | grep sub-workflow | wc -l) -gt 0 ] && cat $WORKFLOW_DEF

	# Pull subworkflow XMLs from the HDFS (runs in a loop until no more subworkflows are found)
	append_sub_workflows $WORKFLOW_DEF 1
}

get_workflow_conf (){
	WORKFLOW=$1
        WORKFLOW_CONF=$2

	$VERBOSE && echo get_workflow_conf $WORKFLOW $WORKFLOW_CONF
	oozie job -oozie http://172.29.0.243:11000/oozie/ -configcontent $WORKFLOW > $WORKFLOW_CONF
}

get_workflow_jobtypes (){
	WORKFLOW_DEF=$1

	$VERBOSE && echo get_workflow_jobtypes $WORKFLOW_DEF
	echo "Here are the workflow types for $WORKFLOW_DEF"
	cat $WORKFLOW_DEF | sed 's/^[ ]*//g' | grep -A1 "^<action" | cut -d' ' -f1 | grep -v ^-- | grep -v action | sort | uniq
}

produce_property_file (){
	WORKFLOW_CONF=$1
        WORKFLOW_PROPERTIES=$2

	rm -f $WORKFLOW_PROPERTIES

	$VERBOSE && echo produce_property_file $WORKFLOW_CONF $WORKFLOW_PROPERTIES
	cat $WORKFLOW_CONF | sed 's/^[ ]*//g' > $TEMP/produce_property_file.xml
	
	# Remove non-ascii characters
	# sed -i 's/[\d128-\d255]//g' $TEMP/produce_property_file.xml
	sed -i 's/[^a-z<>/.:0-9]//ig' $TEMP/produce_property_file.xml
	# perl -i.bak -pe 's/[^[:ascii:]]//g' $TEMP/produce_property_file.xml
	
	grep -n "^<property" $TEMP/produce_property_file.xml | cut -d':' -f1 > $TEMP/produce_property_file.rows
	$PROPERTY_TEST_MODE && head -3 $TEMP/produce_property_file.rows > $TEMP/tmp && cat $TEMP/tmp > $TEMP/produce_property_file.rows
	while read TOP; do
		BOTTOM=$(cat $TEMP/produce_property_file.xml | tail -n +$TOP | grep -n "^</property" | head -1 | cut -d':' -f1)
		cat $TEMP/produce_property_file.xml | tail -n +$TOP | head -$BOTTOM > $TEMP/produce_property_file.property
		grep "^<name" $TEMP/produce_property_file.property | sed 's/<[^<>]*>//g' > $TEMP/keyvaluepair
		grep "^<value" $TEMP/produce_property_file.property | sed 's/<[^<>]*>//g' >> $TEMP/keyvaluepair

		KEY=$(grep "^<name" $TEMP/produce_property_file.property | sed 's/<[^<>]*>//g')
		VALUE=$(grep "^<value" $TEMP/produce_property_file.property | sed 's/<[^<>]*>//g')
		
#		$VERBOSE && echo ""
#		$VERBOSE && echo working on $TOP $BOTTOM
#		$VERBOSE && echo cat $TEMP/keyvaluepair
#		$VERBOSE && cat $TEMP/keyvaluepair
#		$VERBOSE && echo cat with print ORS
#		$VERBOSE && cat $TEMP/keyvaluepair | awk '{print}' ORS=' '
#		$VERBOSE && echo ""
#		$VERBOSE && echo "$(grep "^<name" $TEMP/produce_property_file.property | sed 's/<[^<>]*>//g'),$(grep "^<value" $TEMP/produce_property_file.property | sed 's/<[^<>]*>//g')"	
	
		cat $TEMP/keyvaluepair | awk '{print}' ORS=' ' >> $WORKFLOW_PROPERTIES
		echo -e "\n" >> $WORKFLOW_PROPERTIES
	done < $TEMP/produce_property_file.rows
	#cat $WORKFLOW_CONF | sed 's/^[ ]*//g' | grep -A3 "^<property" | grep -v property | grep -v ^--

	cat $WORKFLOW_PROPERTIES | grep -v ^$ | sed "s/ $//g" > $TEMP/tmp
	cat $TEMP/tmp > $WORKFLOW_PROPERTIES
}

find_replace_properties (){
	WORKFLOW_DEF=$1
	WORKFLOW_PROP=$2

	$VERBOSE && echo "find_replace_properties $WORKFLOW_DEF $WORKFLOW_PROP"
	while read PROP; do
		KEY="\${$(echo $PROP | cut -d' ' -f1)}"
		VALUE=$(echo $PROP | cut -d' ' -f2)
		$VERBOSE && echo PROP $PROP
		#echo "cat $WORKFLOW_DEF | grep $KEY"
		#cat $WORKFLOW_DEF | grep $KEY
		#echo "cat $WORKFLOW_DEF | sed \"s/$KEY/$VALUE/g\""
		VALUE_S=$(echo $VALUE | sed 's/\//\\\//g')
		#cat $WORKFLOW_DEF | sed "s/$KEY/$VALUE_S/g" | grep $VALUE
		sed -i "s/$KEY/$VALUE_S/g" $WORKFLOW_DEF
	done < $WORKFLOW_PROP
}

RUNNING_TOTAL=0
pull_sql_files (){
	WORKFLOW_DEF=$1
	ALL_GIVEN_SQL=$2

	cat $WORKFLOW_DEF | awk '{print}' ORS=' ' | grep -o "<script>[^<>]*</script>" | sed 's/<[^<>]*>//g' | egrep -i "\.q$" > $TEMP/pull_sql_files.list
	while read SQL_TO_PULL; do
		#hadoop fs -ls $SQL_TO_PULL
		#hadoop fs -text $SQL_TO_PULL | wc -l
		hadoop fs -text $SQL_TO_PULL >> $ALL_GIVEN_SQL
		#RUNNING_TOTAL=$((RUNNING_TOTAL+$(hadoop fs -text $SQL_TO_PULL | wc -l)))
		#wc -l $ALL_GIVEN_SQL
		#echo RUNNING_TOTAL $RUNNING_TOTAL
	done < $TEMP/pull_sql_files.list

}

sql_to_dot (){
	ALL_GIVEN_SQL=$1
	DEPENDENCIES=$2

	echo cycle complete:
	wc -l $ALL_GIVEN_SQL
}

produce_hive_dependencies (){
	WORKFLOW_DEF=$1
        DEPENDENCIES=$2
	ALL_GIVEN_SQL=$3

	pull_sql_files $WORKFLOW_DEF $ALL_GIVEN_SQL
	sql_to_dot $ALL_GIVEN_SQL $DEPENDENCIES

}

process_workflow_def (){
	WORKFLOW_DEF=$1
	DEPENDENCIES=$2
	ALL_GIVEN_SQL=$3
	ALL_GIVEN_PIG=$4
	WORKFLOW_PROP=$5
	
	produce_hive_dependencies $WORKFLOW_DEF $DEPENDENCIES $ALL_GIVEN_SQL
	produce_pig_dependencies $WORKFLOW_DEF $DEPENDENCIES $ALL_GIVEN_PIG $WORKFLOW_PROP
}

process_workflow_list (){
	WORKFLOW_LIST=$1

	# Start dependency file
	DEPENDENCIES=$TEMP/DEPENDENCIES.dot
        echo "digraph d {" > $DEPENDENCIES

	# Hive libraries for dot file
        DATABASE_COLOURS=$TEMP/DATABASE_COLOURS.library
        TABLE_DIRECTORIES=$TEMP/TABLE_DIRECTORIES.library

        # Ensure libraries start from scratch
        rm -f $DATABASE_COLOURS $TABLE_DIRECTORIES
        touch $DATABASE_COLOURS $TABLE_DIRECTORIES

	# Hive SQL file
	ALL_GIVEN_SQL=$TEMP/ALL_GIVEN_SQL.sql

	# Pig file
	ALL_GIVEN_PIG=$TEMP/ALL_GIVEN_PIG.pig

        rm -f $ALL_GIVEN_SQL $ALL_GIVEN_PIG
        touch $ALL_GIVEN_SQL $ALL_GIVEN_PIG
        #wc -l $ALL_GIVEN_SQL $ALL_GIVEN_PIG

	$VERBOSE && echo process_workflow_list $WORKFLOW_LIST
	while read WORKFLOW; do
		get_workflow_def $WORKFLOW $TEMP/WORKFLOW_DEF

		grep CustomerProfilePut $TEMP/WORKFLOW_DEF

#		get_workflow_conf $WORKFLOW $TEMP/WORKFLOW_CONF
#		$DEV_MODE && get_workflow_jobtypes $TEMP/WORKFLOW_DEF
#		produce_property_file $TEMP/WORKFLOW_CONF $TEMP/PROPERTIES

#		#cat $TEMP/WORKFLOW_CONF | grep -i staging
#		#cat $TEMP/PROPERTIES | grep -i staging

#		find_replace_properties $TEMP/WORKFLOW_DEF $TEMP/PROPERTIES

#		process_workflow_def $TEMP/WORKFLOW_DEF $DEPENDENCIES $ALL_GIVEN_SQL $ALL_GIVEN_PIG $TEMP/PROPERTIES
#		#produce_visual_output $DEPENDENCIES
	done < $WORKFLOW_LIST

	# End dependency file
        echo "}" >> $DEPENDENCIES
}

# Execution

produce_workflow_list $TEMP/WORKFLOW_LIST
#$VERBOSE && cat $TEMP/WORKFLOW_LIST

process_workflow_list $TEMP/WORKFLOW_LIST
