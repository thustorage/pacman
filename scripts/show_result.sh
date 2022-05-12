#! /bin/bash
# extract data from benchmark_out

if [ -n "$1" ]; then
  if ! test -e $1; then
    echo "file does not exist"
    exit
  fi
else
    echo "missing argument"
    exit
fi

echo "Throughput:"
grep "items_per" $1 | awk -F"[ ,]+" '{print $3}'

echo "Get_P99:"
grep "Get_P99" $1 | awk -F"[ ,]+" '{print $3}'

echo "Put_P99:"
grep "Put_P99" $1 | awk -F"[ ,]+" '{print $3}'

echo "Get_P50:"
grep "Get_P50" $1 | awk -F"[ ,]+" '{print $3}'

echo "Put_P50:"
grep "Put_P50" $1 | awk -F"[ ,]+" '{print $3}'

echo "Compaction Bandwidth:"
grep "CompactionThroughput" $1 | awk -F"[ ,]+" '{print $3}'

echo "CPU Usage:"
grep "CPUUsage" $1 | awk -F"[ ,]+" '{print $3}'
