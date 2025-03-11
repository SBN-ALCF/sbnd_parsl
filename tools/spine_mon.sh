#!/bin/bash

#
# Get system information including running spine jobs as JSON output
#
# How to run: ssh into a node, then run to get the instantaneous statistics.
# To create a log, use the jq command in a loop to add the previous log with
# the current output, e.g.,
# `rm -f ~/log.json; touch /tmp/log && watch -n 30 -x bash -c "cp ~/log.json /tmp/log; jq -s add <(./spine_mon.sh) /tmp/log > /tmp/log2 && mv /tmp/log2 ~/log.json"`

echo "{"

# echo "\"timestamp\": $(date +%s),"
echo \"$(date +%s)\": {

# sed command for cpu and pid only
# ^\s*(([0-9]+\.?[0-9]*)|([0-9]*\.[0-9]+))\s*.*\s([0-9]+).*\s(.*\.).*/\4 \5 \1

# get spine PIDs name being run
echo "\"spine\":{"
ps -C python -o %cpu=,pid=,args= | grep -v "defunct" \
    | grep run.py | awk '{printf("\"%s\": {\"PID\":%s ,\"cpu\": %.1f}\n", $2, $2, $1)}' \
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
nvidia-smi --query-gpu=pci.bus_id,name,utilization.gpu,memory.used,memory.total --format=csv,noheader \
    | sed 's/, /,/g' | awk -F"," \
    '{
        printf("\"%s\":{\"name\": \"%s\", \"gpu\": %.1f, \"mem\":%d, \"total_mem\": %d}\n", $1, $2, $3, $4, $5)
    }' | paste -sd,
echo "}"

echo "}}"
