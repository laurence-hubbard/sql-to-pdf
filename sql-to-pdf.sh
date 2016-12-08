#! /bin/bash

# Take a SQL file and turn it into a relationship diagram in PDF form

##
## Configurable
##

# Colours for PDF points
COLOURS="lightcyan,brown1,peru,yellowgreen,orangered2,whitesmoke,salmon,sandybrown,gold,lightblue,yellow,orange,limegreen,pink,grey,mediumturquoise"

# Output file name
DEFAULT_NAME=true
OVERRIDE_NAME="override-example"

# Hard Configs 
alias sed='gsed'
TEMP=/tmp/$(basename $0)
mkdir -p $TEMP
log(){
  echo $(date +%F\ %T) - sql-to-pdf - $1
}

if [ $# -eq 1 ]; then
	INPUT_SQL=$1
else
	log "Test mode enabled (otherwise provide SQL file as an argument)"
	INPUT_SQL=./target/sql/example.sql
fi

$DEFAULT_NAME && FILENAME=$(basename $INPUT_SQL | cut -d'.' -f1) || FILENAME=$OVERRIDE_NAME

log "Starting - converting $INPUT_SQL to ./target/pdf/$FILENAME.pdf!"

SQL=./target/sql
DOT=./target/dot
PDF=./target/pdf

NODE_COUNT=1
CURRENT_DATABASE=DEFAULT
USE_COLOURS=true

OUTPUT_FORMAT=pdf
OUTPUT_FILE=$PDF/$FILENAME.$OUTPUT_FORMAT
DOT_FILE=$DOT/$FILENAME.dot

# Colour picking mechanism
X=1
increment() {
	[ $X -eq $(($(echo $COLOURS | grep -o "," | wc -l)+1)) ] && X=1 || X=$((X+1))
}
colour() {
	COLOUR=""
	COLOUR=$(echo $COLOURS | cut -d',' -f$X)
}

DB_FILE=$TEMP/DB_FILE.colours
rm -f $DB_FILE
touch $DB_FILE

db_colour (){
	COL=""
	COLOUR=""
	if [ $(grep -c "^$1," $DB_FILE) -ne 0 ]; then
		COL=$(grep "^$1," $DB_FILE | cut -d',' -f2)
	else
		colour
		COL=$COLOUR
		increment
	fi
	echo $1,$(echo $COL | cut -d' ' -f1) >> $DB_FILE
	COL=$(echo $COL | cut -d' ' -f1)
}

sql_targets () {
	# This function pulls out all data flow TARGETS from a given SQL file, outputting to STDOUT
	INPUT_FILE=$1

# Create table MYTABLE as
cat $INPUT_FILE | grep -vi "if not exists"  | egrep -io "create table .* as"  | sed 's/create table //ig' | cut -d' ' -f1 | grep -v ^$ | tr '[:lower:]' '[:upper:]'
cat $INPUT_FILE | grep "if not exists"  | egrep -io "create table .* as" | sed 's/create table if not exists //ig' | cut -d' ' -f1 | grep -v ^$ | tr '[:lower:]' '[:upper:]'

# Insert into table
cat $INPUT_FILE  | egrep -io "INSERT INTO TABLE [^ ]*" | sed 's/INSERT INTO TABLE //ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'

# Insert overwrite table
cat $INPUT_FILE  | egrep -io "INSERT overwrite TABLE [^ ]*" | sed 's/INSERT overwrite TABLE //ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'

# Alter table MYTABLE rename to NEWTABLE
cat $INPUT_FILE | egrep -io "ALTER TABLE [^ ]* RENAME TO [^ ]*" | grep -io "RENAME TO [^ ]*" | sed 's/RENAME TO //ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'

# Create external table MYTABLE as
cat $INPUT_FILE  | egrep -io "create external table [^ ]* as" | sed 's/create external table //ig' | sed 's/ as//ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'

# Create table if not exists MYTABLE as
cat $INPUT_FILE  | egrep -io "create table if not exists [^ ]* as" | sed 's/create table if not exists //ig' | sed 's/ as//ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'

# Create external table if not exists MYTABLE as
cat $INPUT_FILE  | egrep -io "create external table if not exists [^ ]* as" | sed 's/create external table if not exists //ig' | sed 's/ as//ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'

# Create view MYVIEW as
cat $INPUT_FILE  | egrep -io "create view [^ ]* as" | sed 's/create view //ig' | sed 's/ as//ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'

# Insert overwrite directory
cat $INPUT_FILE  | egrep -io "insert overwrite directory [^ ]*"| sed 's/INSERT overwrite directory //ig' | sed "s/'//g" | grep -v ^$ | tr '[:lower:]' '[:upper:]'

}

sql_sources () {
INPUT_FILE=$1

cat $INPUT_FILE | egrep -io " from [^ ()]*|join [^ ()]*" | sed 's/ from //ig' | sed 's/join //ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'
cat $INPUT_FILE | grep -io "ALTER TABLE [^ ]* RENAME TO" | sed 's/ALTER TABLE //ig' | sed 's/ rename to//ig' | grep -v ^$ | tr '[:lower:]' '[:upper:]'

}

extract_value_from_sql_file (){
INPUT_FILE=$1

#               remove comments                 file on 1 line          split SQL parts         remove drop             remove comments (again?)         remove use      remove blank & alter    remove tabs & squash multi-spaces
cat $INPUT_FILE | sed 's/^[ ]*//g' | grep -v ^- | awk '{print}' ORS=' ' | sed 's/;/\n/g' | grep -iv ^set | grep -vi "drop table" | sed 's/^[ ]*//g' | grep -v ^- | grep -v ^$ | grep -iv ^alter | sed 's/\t/ /g' | tr -s ' ' | grep -iv ^set > $TEMP/extract_value_from_sql_file.tmp
cat $INPUT_FILE | sed 's/^[ ]*//g' | grep -v ^- | awk '{print}' ORS=' ' | sed 's/;/\n/g' | egrep -io "ALTER TABLE [^ ]* RENAME TO [^ ]*" | sed 's/\t/ /g' | tr -s ' ' >> $TEMP/extract_value_from_sql_file.tmp

# The alter table statements need to be INLINE, otherwise the databases used are out of order:
cat $INPUT_FILE | sed 's/\t/ /g' | tr -s ' ' | sed 's/^[ ]*//g' | grep -v ^- | awk '{print}' ORS=' ' | sed 's/;/\n/g' | grep -iv ^set | grep -vi "drop table" | grep -v ^$ > $TEMP/extract_value_from_sql_file.tmp

echo $TEMP/extract_value_from_sql_file.tmp

echo "digraph d {" > $DOT_FILE

while read LINE; do
	[ $(echo $LINE | grep -i ^use | wc -l) -ne 0 ] && CURRENT_DATABASE=$(echo $LINE | grep -io "use [^ ]*" | sed 's/use //ig' | tr '[:lower:]' '[:upper:]')  && continue
	echo $LINE > $TEMP/tmp
	echo $LINE
	sql_sources $TEMP/tmp > $TEMP/sources
	sql_targets $TEMP/tmp > $TEMP/targets
	echo sources
	cat $TEMP/sources
	echo targets
	cat $TEMP/targets
	SOURCES=""
	while read SOURCE; do
		SOURCE=$(echo $SOURCE | sed 's/^[()]//g' | sed 's/[()]$//g')
#		SOURCES="$SOURCES $NODE_COUNT"
#		SOURCE=$(echo $SOURCE | sed 's/\./__/g')
		[ $(echo $SOURCE | grep -o "\." | wc -l) -eq 0 ] && SOURCE="$CURRENT_DATABASE"."$SOURCE"
		SOURCE="\"$SOURCE\""
		SOURCES="$SOURCES $SOURCE"
	#	echo "$NODE_COUNT [label =\"$SOURCE\"]" >> $DOT/out.dot
		NODE_COUNT=$((NODE_COUNT+1))
		SOURCE=$(echo $SOURCE | sed 's/"//g')
		DATABASE=$(echo $SOURCE | cut -d'.' -f1)
		db_colour $DATABASE
		$USE_COLOURS && echo "\"$SOURCE\" [shape=box,style=filled,color="$COL"]" >> $DOT_FILE
	done < $TEMP/sources
	TARGETS=""
	while read TARGET; do
		TARGET=$(echo $TARGET | sed 's/^[()]//g' | sed 's/[()]$//g')
	#	TARGETS="$TARGETS $NODE_COUNT"
#		TARGET=$(echo $TARGET | sed 's/\./__/g')
		[ $(echo $TARGET | egrep -o "\.|/" | wc -l) -eq 0 ] && TARGET="$CURRENT_DATABASE"."$TARGET" 
		TARGET="\"$TARGET\""
		TARGETS="$TARGETS $TARGET"
	#	echo "$NODE_COUNT [label =\"$TARGET\"]" >> $DOT/out.dot
                NODE_COUNT=$((NODE_COUNT+1))
		TARGET=$(echo $TARGET | sed 's/"//g')
		DATABASE=$(echo $TARGET | cut -d'.' -f1)
		db_colour $DATABASE
                $USE_COLOURS && echo "\"$TARGET\" [shape=box,style=filled,color="$COL"]" >> $DOT_FILE
	done < $TEMP/targets
#	echo $SOURCES
#	echo $TARGETS
	echo "{ $SOURCES } -> { $TARGETS }" >> $DOT_FILE
done < $TEMP/extract_value_from_sql_file.tmp

echo "}" >> $DOT_FILE
cat $DOT_FILE

dot -T $OUTPUT_FORMAT -O $DOT_FILE -o $OUTPUT_FILE

}

extract_value_from_sql_file $INPUT_SQL
