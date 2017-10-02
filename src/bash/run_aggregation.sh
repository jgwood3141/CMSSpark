#!/bin/sh

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') $1" >> cron_log.txt
    echo "$(date +'%Y-%m-%d %H:%M:%S') $1"
}

last_non_temp_short_date() {
    # hadoop fs -ls -R $1 - get list of all files and directories in $1 (recursively)
    # grep -E ".*[0-9]{4}/[0-9]{2}/[0-9]{2}$" - get only lines that end with dddd/dd/dd (d - digit)
    # tail -n1 - get the last line (last directory)
    # tail -c11 - get last 11 characters (+1 for newline)
    # sed -e "s/\///g" - replace all / with nothing (delete /)
    result=$(hadoop fs -ls -R $1 | grep -E ".*[0-9]{4}/[0-9]{2}/[0-9]{2}$" | tail -n1 | tail -c11 | sed -e "s/\///g")

    echo $result
    return 0
}

last_non_temp_long_date() {
    # hadoop fs -ls -R $1 - get list of all files and directories in $1 (recursively)
    # sort -n - sort entries by comparing according to string numerical value
    # grep -E ".*year=[0-9]{4}/month=[0-9]{1,2}/day=[0-9]{1,2}$" - get only lines that end with year=dddd/month=dd/day=dd (d - digit)
    # tail -n1 - get the last line (last directory)
    # cut -d "=" -f 2- - get substring from first =
    # sed -E "s/[a-z]*=[0-9]{1}(\/|$)/0&/g" - replace all word=d (d - digit) with 0word=d
    # sed -E "s/[^0-9]//g" - delete all characters that are not digits
    result=$(hadoop fs -ls -R $1 | sort -n | grep -E ".*year=[0-9]{4}/month=[0-9]{1,2}/day=[0-9]{1,2}$" | tail -n1 | cut -d "=" -f 2- | sed -E "s/[a-z]*=[0-9]{1}(\/|$)/0&/g" | sed -E "s/[^0-9]//g")
    echo $result
    return 0
}

if [ $# -le 1 ] || [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ]; then
    echo "Usage: run_aggregation.sh <configuration> <date>"
    echo "Example: PYTHONPATH=/path/CMSSpark/src/python ./run_aggregation.sh conf.json 20170925"
    exit 0
fi
conf=$1

# find out where CMSSpark is installed on a system
mroot=`python -c "import CMSSpark; print '/'.join(CMSSpark.__file__.split('/')[:-1])" 2>&1`
err=`echo $mroot | grep ImportError`
if [ -n "$err" ]; then
    echo "Unable to find CMSSpark"
    echo $mroot
    exit 1
fi

cmsspark=`echo $mroot | sed -e "s,/src/python/CMSSpark,,g"`
echo "CMSSpark is located at $cmsspark"
echo "Read configuration: $conf"
export PATH=$cmsspark/bin:$PATH
export PYTHONPATH=$mroot:$PYTHONPATH

aaa_date=""
eos_date=""
cmssw_date=""
jm_date=""

if [ "$2" != "" ]; then
    aaa_date="$2"
    eos_date="$aaa_date"
    cmssw_date="$aaa_date"
    jm_date="$aaa_date"
else
    aaa_date=$(last_non_temp_short_date $aaa_dir)
    eos_date=$(last_non_temp_short_date $eos_dir)
    cmssw_date=$(last_non_temp_long_date $cmssw_dir)
    jm_date=$(last_non_temp_long_date $jm_dir)
fi

log "----------------------------------------------"
log "Starting script"

log "AAA date $aaa_date"
log "CMSSW date $cmssw_date"
log "EOS date $eos_date"
log "JM date $jm_date"

# parse configuration

output_dir=`cat $conf | python -c "import sys, json; print json.load(sys.stdin)['output_dir']"`
stomp_path=`cat $conf | python -c "import sys, json; print json.load(sys.stdin)['stomp_path']"`
credentials=`cat $conf | python -c "import sys, json; print json.load(sys.stdin)['credentials']"`
keytab=`cat $conf | python -c "import sys, json; print json.load(sys.stdin)['keytab']"`

aaa_dir=`cat conf.json | python -c "import sys, json; print json.load(sys.stdin)['aaa_dir']"`
cmssw_dir=`cat conf.json | python -c "import sys, json; print json.load(sys.stdin)['cmssw_dir']"`
eos_dir=`cat conf.json | python -c "import sys, json; print json.load(sys.stdin)['eos_dir']"`
jm_dir=`cat conf.json | python -c "import sys, json; print json.load(sys.stdin)['jm_dir']"`

log "AAA $aaa_dir"
log "CMSSW $cmssw_dir"
log "EOS $eos_dir"
log "CRAB $jm_dir"

# Kerberos
principal=`klist -k "$keytab" | tail -1 | awk '{print $2}'`
kinit $principal -k -t "$keytab"

if [ $aaa_date != "" ] && [ $aaa_date == $cmssw_date ] && [ $cmssw_date == $eos_date ] && [ $eos_date == $jm_date ]; then
    log "All streams are ready for $aaa_date"

    output_dir_with_date=$output_dir"/"${aaa_date:0:4}"/"${aaa_date:4:2}"/"${aaa_date:6:2}
    log "Output directory $output_dir_with_date"
    output_dir_ls=$(hadoop fs -ls $output_dir_with_date | tail -n1)
    if [ "$output_dir_ls" != "" ]; then
        log "Output at $output_dir_with_date exist, will cancel"
    else
        log "Output at $output_dir_with_date does not exist, will run"

        # Add --verbose for verbose output
        run_spark data_aggregation.py --yarn --date "$aaa_date" --fout "$output_dir"
        run_spark cern_monit.py --hdir "$output_dir_with_date" --stomp="$stomp_path" --amq "$credentials" --verbose --aggregation_schema
    fi

else
    log "Not running script because not all streams are ready"
fi
 
log "Finishing script"
