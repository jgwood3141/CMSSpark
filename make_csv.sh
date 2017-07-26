#!/bin/bash

# Input Sanitation
if [ $# != 6 ]; then
    echo "Args for this script are: "
    echo "  -din /path/to/spark/output/ "
    echo "  -fout /path/to/write/output/not/on/analytix/fileOutputName.csv "
    echo "  -grepvkey keyToRemoveHeaderLines "
    return 1
fi

# Grap args
while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -din|--input_directory)
    DIN="$2"
    shift # past argument
    ;;
    -fout|--output_file)
    FOUT="$2"
    DOUT=$(dirname "${2}$")
    shift # past argument
    ;;
    -grepvkey|--grep_v_key)
    GREPVKEY="$2"
    shift # past argument
    ;;
esac
shift # past argument or value
done

# Print args for user
echo "INPUT DIRECTORY = $DIN"
echo "GREP V KEY      = $GREPVKEY"
echo "OUTPUT FILE     = $FOUT"

# Check if output directory exists
if [ ! -d $DOUT ]; then mkdir -p $DOUT; fi

# Get header of input 
hadoop fs -cat $DIN/part-00001 | head -1 > $FOUT

# Get rest of files, grep -v to avoid headers, sort and uniq do what you think
hadoop fs -cat $DIN/* | grep -v $GREPVKEY | sort | uniq >> $FOUT

# Exit Succsefully 
return 0
