#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

# to avoid no available space
./clean_pmem_dir.sh

########## configurations ##########

# basic
INDEX_TYPE=1  # INDEX TYPE: 1 CCEH 2 FastFair 3 Masstree
# index is on PM or not
IDX_PERSISTENT="ON" # ON OFF
# log is on PM or not
LOG_PERSISTENT="ON" # ON OFF
LOG_BATCHING="ON" # ON OFF. simulate FlatStore's batching

# PACMAN option
REDUCE_PM_ACCESS="ON" # ON OFF
HOT_COLD_SEPARATE="ON"  # ON OFF
GC_SHORTCUT="ON"  # ON OFF
BATCH_COMPACTION="ON"  # ON OFF

# benchmark config
INIT_UTIL="80"  # 0 50 60 70 80 90. capacity utilization
THREADS="12"  # 1~32. threads for workloads
NUM_KEYS="200000000"
NUM_OPS_PER_THREAD="20000000"
NUM_WARMUP_OPS_PER_THREAD="0"
VALUE_SIZE="48"
NUM_GC_THREADS="4"
WORKLOAD_TYPE="YCSB"  # YCSB ETC
YCSB_TYPE="YCSB_A" # YCSB_A, YCSB_B, YCSB_C, YCSB_E, YCSB_W0, YCSB_W20, YCSB_W40, YCSB_W60, YCSB_W80, YCSB_W100
SKEW="true" # true (Zipfian), false (uniform)

MEASURE_LATENCY="OFF"  # ON OFF
USE_ALL_CORES="OFF" # ON OFF. If set, num_gc_threads will be (NUM_ALL_CORES - num_worker_threads)
TEST_LOAD="OFF" # ON OFF. If set, only evaluate random loading phase

####################################

NUMA_AFFINITY=0 # running on which NUMA node
FILTER="--benchmark_filter=/($INIT_UTIL)/.*/threads:($THREADS)$"

mkdir -p ../results
mkdir -p ../build
cd ../build

OUTPUT_FILE=../results/case

# it may take long to get third-party dependencies, so don't delete _deps
ls | grep -v _deps | xargs rm -rf

# build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_NUMA_NODE=${NUMA_AFFINITY} \
  -DINDEX_TYPE=${INDEX_TYPE} -DIDX_PERSISTENT=${IDX_PERSISTENT} \
  -DLOG_PERSISTENT=${LOG_PERSISTENT} -DLOG_BATCHING=${LOG_BATCHING} \
  -DREDUCE_PM_ACCESS=${REDUCE_PM_ACCESS} \
  -DHOT_COLD_SEPARATE=${HOT_COLD_SEPARATE} -DGC_SHORTCUT=${GC_SHORTCUT} \
  -DBATCH_COMPACTION=${BATCH_COMPACTION} \
  -DNUM_KEYS=${NUM_KEYS} -DNUM_OPS_PER_THREAD=${NUM_OPS_PER_THREAD} \
  -DNUM_WARMUP_OPS_PER_THREAD=${NUM_WARMUP_OPS_PER_THREAD} \
  -DVALUE_SIZE=${VALUE_SIZE} -DNUM_GC_THREADS=${NUM_GC_THREADS} \
  -DWORKLOAD_TYPE=${WORKLOAD_TYPE} -DYCSB_TYPE=${YCSB_TYPE} -DSKEW=${SKEW} \
  -DMEASURE_LATENCY=${MEASURE_LATENCY} -DUSE_ALL_CORES=${USE_ALL_CORES} \
  -DTEST_LOAD=${TEST_LOAD} ..

make pacman_bench -j

# disable cpu scaling
sudo cpupower frequency-set --governor performance > /dev/null
# clean cache
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
  ./benchmarks/pacman_bench --benchmark_repetitions=1 ${FILTER} \
  --benchmark_out=${OUTPUT_FILE} --benchmark_out_format=json

sudo cpupower frequency-set --governor powersave > /dev/null
