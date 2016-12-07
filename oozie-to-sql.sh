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

while read WORKFLOW_ID; do
    log "Getting workflow defintion for \$WORKFLOW_ID"
    oozie job -oozie http://172.29.0.243:11000/oozie/ -definition \$WORKFLOW_ID > $PROXY_LANDING/workflow_definition.temp
    
    log "Looking for sub-workflows in \$WORKFLOW_ID"
    cat $PROXY_LANDING/workflow_definition.temp | awk '{print}' ORS=' ' | grep -o "<app-path>[^<>]*</app-path>" | sed 's/<[^<>]*>//g' | sed 's/^\${[^{}]*}//g' | sed 's|$|/workflow.xml|g' > $PROXY_LANDING/sub_workflows_list.temp
    if [ \$(cat $PROXY_LANDING/sub_workflows_list.temp | wc -l) -ne 0 ]; then
        log "Sub-workflow(s) found"
        while read SUB_WORKFLOW_HDFS_PATH; do
            log "Adding sub-workflow XML \$SUB_WORKFLOW_HDFS_PATH to definition of \$WORKFLOW_ID"

#  CAN'T GET THIS BIT TO WORK!!!

            sudo su hdfs "hadoop fs -text $SUB_WORKFLOW_HDFS_PATH" | awk '{print}' ORS=' ' | grep -o "<app-path>[^<>]*</app-path>" | sed 's/<[^<>]*>//g' | sed 's/^\${[^{}]*}//g' | sed 's|$|/workflow.xml|g' >> $PROXY_LANDING/workflow_definition.temp 
            sudo su hdfs chmod 777 $PROXY_LANDING/workflow_definition.temp
        done < $PROXY_LANDING/sub_workflows_list.temp
    else
        log "No sub-workflows found"
    fi

done < $PROXY_LANDING/workflow_ids.txt

RUN


