#!/bin/bash

#
# Get system information including running lar jobs as JSON output
#
# How to run: ssh into a node, then run to get the instantaneous statistics.
# To create a log, use the jq command in a loop to add the previous log with
# the current output, e.g.,
# `watch -n 30 -x bash -c "touch /tmp/log; mv log.json /tmp/log; jq -s add <(./lar_mon.sh) /tmp/log > log.json"`

echo "{"

# echo "\"timestamp\": $(date +%s),"
echo \"$(date +%s)\": {

# get lar PIDs & fcl name being run
echo "\"lar\":{"
ps -C lar -o %cpu=,pid=,args= | grep -v "defunct" \
    | sed -r 's/^\s*(([0-9]+\.?[0-9]*)|([0-9]*\.[0-9]+))\s*.*\s([0-9]+).*\s(.*\.fcl).*/\4 \5 \1/g' \
    | awk '{printf("\"%d\":{\"fcl\": \"%s\", \"cpu\": %.1f}\n", $1, $2, $3)}' \
    | paste -sd ","
echo "},"

# cpu info 10 s interval
mpstat -P ALL -o JSON 10 1 | tr '\n' '\r' | sed 's/^.\(.*\)..$/\1/mg'

# memory
free -k | awk '{if($1=="Mem:"){printf(",\"mem\":{\"total\":%d,\"used\":%d,\"free\":%d}\n", $2, $3, $4)}}'

# disk
echo ",\"disk\":"
jq '.sysstat.hosts[0].statistics[0].disk' <(iostat -o JSON)

# gpu info
echo ",\"gpu\": {"
nvidia-smi -q -d UTILIZATION | egrep -A 3 "^GPU" | sed 's/--//g' | sed -r '/^\s*$/d' \
    | awk '{
            if((NR-1)%4==0){
                printf("\"%s\":{", $2)
            }
            else if((NR-1)%4==2){
                printf("\"gpu\": %.1f,"), $3
            }
            else if((NR-1)%4==3){
                printf("\"memory\": %.1f}\n"), $3
            }
        }' \
    | paste -sd","
echo "}"

echo "}}"
