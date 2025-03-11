# script for looping the mon scripts
# pass name of mon script as argument
# e.g. ./loop.sh spine_mon.sh
rm -f ~/log.json; rm -f /tmp/log; touch /tmp/log && watch -n 10 -x bash -c "cp ~/log.json /tmp/log; jq -s add <(./$1) /tmp/log > /tmp/log2 && mv /tmp/log2 ~/log.json"
