#!/bin/bash

if [ $# -ne 5 ];then
    echo "Launch Jenkins job and wait for completion while copying repository contents to remote_host."
    echo "5 params expected: \nN1 jobname\nN2 token\nN3 repo tag\nN4 remote_host\nN5 destination path filename w/o ext"
    exit 1
fi

jobname=$1
token=$2
tag=$3
remote_host=$4
archive_name=$5.tgz

JENKINS=${JENKINS:-https://ci.caspian.rax.io}
JOB_URL=$JENKINS/job/$jobname
JOB_STATUS_URL=${JOB_URL}/lastBuild/api/json

GREP_RETURN_CODE=0

# Start the build
crumb=`wget -q --output-document - "$JENKINS/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,\":\",//crumb)"`
res=`curl --header "$crumb" -X POST $JOB_URL/build --data token=$token --data-urlencode json="{\"parameter\":[{\"name\":\"remote_host\", \"value\":\"$remote_host\"}, {\"name\":\"archive_name\", \"value\":\"$archive_name\"}, {\"name\":\"tag\", \"value\":\"$tag\"}]}" 2>/dev/null`

if [ "$res" != "" ];then
    echo Error
    echo "$res"
    exit 1
fi

# Poll until the build is finished
while [ $GREP_RETURN_CODE -eq 0 ]
do
    sleep 30
    # Grep will return 0 while the build is running:
    status=`curl --silent $JOB_STATUS_URL`
    echo $status | grep result\":null > /dev/null
    GREP_RETURN_CODE=$?
done

echo $status | grep result\":\"SUCCESS > /dev/null
GREP_RETURN_CODE=$?
stat $archive_name
STAT_CODE=$?
if [ $GREP_RETURN_CODE -eq 0 ] && [ $STAT_CODE -eq 0 ];then
    echo $jobname Build SUCCESS
else
    echo $jobname Build FAILURE
    exit 1
fi
