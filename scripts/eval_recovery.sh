#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

help() {
  echo "Usage: $0 <db_type>"
  echo "  <db_type>: 1: FlatStore-H, 2: FlatStore-PH, 3: FlatStore-FF, 4: FlatStore-M"
}

if [[ $# != 1 || $1 < 1 || $1 > 4 ]]; then
  help
  exit
fi

# to avoid no available space
./clean_pmem_dir.sh

if [[ $1 == 1 || $1 == 2 ]]; then
  INDEX_TYPE=1
elif [[ $1 == 3 ]]; then
  INDEX_TYPE=2
elif [[ $1 == 4 ]]; then
  INDEX_TYPE=3
fi

if [[ $1 == 1 || $1 == 4 ]]; then
  IDX_PERSISTENT="-DIDX_PERSISTENT=OFF"
else
  IDX_PERSISTENT="-DIDX_PERSISTENT=ON"
fi

NUMA_AFFINITY=0

NUM=200000000
NUM_OPS=100000000
INIT_UTIL=80
VALUE_SIZE=256

SERVICE_THREADS=24  # number of workload threads
GC_THREADS=8        # number of compaction threads and recovery threads

mkdir -p ../results
mkdir -p ../build
cd ../build

# it may take long to get third-party dependencies, so don't delete _deps
ls | grep -v _deps | xargs rm -rf
# build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_NUMA_NODE=$NUMA_AFFINITY \
  -DINDEX_TYPE=${INDEX_TYPE} ${IDX_PERSISTENT} -DPACMAN=ON ..

make recovery_test -j

# disable cpu scaling
sudo cpupower frequency-set --governor performance > /dev/null
# clean cache
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

numactl --membind=$NUMA_AFFINITY --cpunodebind=$NUMA_AFFINITY \
./benchmarks/recovery_test --num=$NUM --num_ops=$NUM_OPS --threads=$SERVICE_THREADS --gc_threads=$GC_THREADS --value_size=$VALUE_SIZE --init_util=$INIT_UTIL

sudo cpupower frequency-set --governor powersave > /dev/null
