#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

help() {
  echo "Usage: $0 <db_type> <apply_pacman>"
  echo "  <db_type>: 1: FlatStore-H, 2: FlatStore-PH, 3: FlatStore-FF, 4: FlatStore-M, 5: Viper, 6: ChameleonDB 7: PMem-RocksDB 8: pmemkv"
  echo "  <apply_pacman>: 0: false, 1: true (not affects PMem-RocksDB and pmemkv)"
}

if [[ $# == 1 ]]; then
  if [[ $1 > 0 && $1 < 6 ]]; then
    help
    exit
  fi
elif [[ $# != 2 || $1 < 1 || $1 > 8 || ($2 != 0 && $2 != 1) ]]; then
  help
  exit
fi

# to avoid no available space
./clean_pmem_dir.sh

INDEX_TYPE=1
if [[ $1 == 1 || $1 == 2 || $1 == 5 ]]; then
  INDEX_TYPE=1
elif [[ $1 == 3 ]]; then
  INDEX_TYPE=2
elif [[ $1 == 4 ]]; then
  INDEX_TYPE=3
fi

if [[ $1 == 1 || $1 == 4 || $1 == 5 ]]; then
  IDX_PERSISTENT="-DIDX_PERSISTENT=OFF"
else
  IDX_PERSISTENT="-DIDX_PERSISTENT=ON"
fi

if [[ $1 -le 4 ]]; then
  TARGET="pacman_bench"
  TARGET_CMD="./benchmarks/pacman_bench"
else
  WITH_OTHERS="-DEVAL_OTHER_SYSTEMS=ON"
  if [[ $1 == 5 ]]; then
    TARGET="viper_bench"
  elif [[ $1 == 6 ]]; then
    TARGET="chameleondb_bench"
  elif [[ $1 == 7 ]]; then
    TARGET="pmem_rocksdb_bench"
  elif [[ $1 == 8 ]]; then
    TARGET="pmemkv_bench"
  fi
    TARGET_CMD="./benchmarks/other/${TARGET}"
fi

PACMAN_OPT=""
if [[ $2 == 1 ]]; then
  PACMAN_OPT="-DPACMAN=ON"
fi

NUMA_AFFINITY=0
LOG_BATCHING=OFF # simulate FlatStore's batching (if LOG_PERSISTENT), diabled for fair comparison
NUM_KEYS=200000000
NUM_OPS_PER_THREAD=20000000

mkdir -p ../results
mkdir -p ../build
cd ../build

THREADS=24
if [[ $1 -le 6 ]]; then
  FILTER="--benchmark_filter=/(80)/.*/threads:(${THREADS})$"
  OUTPUT_FILE=../results/etc_$1_$2
else
  FILTER="--benchmark_filter=/.*/threads:(${THREADS})$"
  OUTPUT_FILE=../results/etc_$1
fi
# clean the result file

# it may take long to get third-party dependencies, so don't delete _deps
ls | grep -v _deps | xargs rm -rf
# build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_NUMA_NODE=${NUMA_AFFINITY} \
  ${WITH_OTHERS} -DINDEX_TYPE=${INDEX_TYPE} ${IDX_PERSISTENT} \
  -DLOG_BATCHING=${LOG_BATCHING} ${PACMAN_OPT} \
  -DNUM_KEYS=${NUM_KEYS} -DNUM_OPS_PER_THREAD=${NUM_OPS_PER_THREAD} \
  -DNUM_GC_THREADS=4 -DWORKLOAD_TYPE=ETC -DMEASURE_LATENCY=ON ..
make ${TARGET} -j

# disable cpu scaling
sudo cpupower frequency-set --governor performance > /dev/null
# clean cache
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
  ${TARGET_CMD} --benchmark_repetitions=1 ${FILTER} \
  --benchmark_out=${OUTPUT_FILE} --benchmark_out_format=json

sudo cpupower frequency-set --governor powersave > /dev/null
