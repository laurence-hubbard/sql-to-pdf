#! /bin/bash

# Log onto an Hadoop Cluster and retrieve SQL scripts which are run in production.

# Configurable
OOZIE_URL="http://172.29.0.243:11000/oozie/"
PROXY_SSH="ssh INSURANCE\\L_Hubbard@172.29.0.243 -i /Users/LHubbard/.ssh/hadoop_key"
PROXY_HOST="INSURANCE\\L_Hubbard@172.29.0.243"
PROXY_KEY="/Users/LHubbard/.ssh/hadoop_key"
PROXY_LANDING=/home/likewise-open/INSURANCE/l_hubbard/LANDING

# Hard Configs
PROXY_SQL_OUT=$PROXY_LANDING/OOZIE_WORKFLOWS.sql
OOZIE_JOBS_HISTORY=1000
TEST=true
TEST_VOLUME=1

$PROXY_SSH <<RUN

log(){
  echo \$(date +%F\ %T) - oozie-to-sql - \$1
}

$TEST && log "Test mode enabled and test volume is $TEST_VOLUME"

log "Starting!!!!"

log "Getting unique list of workflow names"
oozie jobs -oozie http://172.29.0.243:11000/oozie/ -len $OOZIE_JOBS_HISTORY | grep -v ^- | awk '{print \$2}' | grep SUCCEEDED$ | sed 's/SUCCEEDED$//g' | sort | uniq > $PROXY_LANDING/workflow_names.txt

$TEST && head -$TEST_VOLUME $PROXY_LANDING/workflow_names.txt > $PROXY_LANDING/workflow_names.txt.temp && cat $PROXY_LANDING/workflow_names.txt.temp > $PROXY_LANDING/workflow_names.txt

log "Getting unique list of workflow ids"
rm -f $PROXY_LANDING/workflow_ids.txt
while read WORKFLOW_NAME; do
    log "Getting workflow ID for \$WORKFLOW_NAME"
    oozie jobs -oozie http://172.29.0.243:11000/oozie/ -len $OOZIE_JOBS_HISTORY | grep "\$WORKFLOW_NAME"SUCCEEDED | head -1 | awk '{print \$1}' >> $PROXY_LANDING/workflow_ids.txt
done < $PROXY_LANDING/workflow_names.txt

log "Getting set of workflow defintions, including sub-workflow definitions"

rm -f $PROXY_LANDING/hive_ql_full_list.txt
while read WORKFLOW_ID; do
    log "Getting workflow defintion for \$WORKFLOW_ID"
    oozie job -oozie http://172.29.0.243:11000/oozie/ -definition \$WORKFLOW_ID > $PROXY_LANDING/workflow_definition.temp
    
    log "Looking for sub-workflows in \$WORKFLOW_ID"
    cat $PROXY_LANDING/workflow_definition.temp | awk '{print}' ORS=' ' | grep -o "<app-path>[^<>]*</app-path>" | sed 's/<[^<>]*>//g' | sed 's/^\${[^{}]*}//g' | sed 's|$|/workflow.xml|g' > $PROXY_LANDING/sub_workflows_list.temp
    if [ \$(cat $PROXY_LANDING/sub_workflows_list.temp | wc -l) -ne 0 ]; then
        log "Sub-workflow(s) found"
        while read SUB_WORKFLOW_HDFS_PATH; do
            log "Adding sub-workflow XML \$SUB_WORKFLOW_HDFS_PATH to definition of \$WORKFLOW_ID"

sudo su hdfs <<TEXT >> $PROXY_LANDING/workflow_definition.temp
                hadoop fs -text \$SUB_WORKFLOW_HDFS_PATH
TEXT

        done < $PROXY_LANDING/sub_workflows_list.temp
    else
        log "No sub-workflows found"
    fi
    
    log "Extracting HiveQL paths from workflow definition"
    cat $PROXY_LANDING/workflow_definition.temp | awk '{print}' ORS=' ' | grep -o "<script>[^<>]*</script>" | sed 's/<[^<>]*>//g' | egrep -i "\.q$" >> $PROXY_LANDING/hive_ql_full_list.txt
done < $PROXY_LANDING/workflow_ids.txt

cat $PROXY_LANDING/hive_ql_full_list.txt | sort | uniq > $PROXY_LANDING/hive_ql_uniq_list.txt
log "Number of unique HiveQL files is \$(cat $PROXY_LANDING/hive_ql_uniq_list.txt | wc -l)"

log "Extracting SQL from files found"
rm -f $PROXY_LANDING/bretton-cluster.sql
while read HIVE_QL; do
log "Reading SQL from \$HIVE_QL"
sudo su hdfs <<TEXT >> $PROXY_LANDING/bretton-cluster.sql
hadoop fs -text \$HIVE_QL
TEXT
done < $PROXY_LANDING/hive_ql_uniq_list.txt

log "Now downloading SQL file"

RUN

rm -f ./target/sql/bretton-cluster.sql
scp -i $PROXY_KEY $PROXY_HOST:$PROXY_LANDING/bretton-cluster.sql ./target/sql/bretton-cluster.sql
