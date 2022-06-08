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

PACMAN_OPT=""
OPT=(
  ""
  "-DREDUCE_PM_ACCESS=ON"
  "-DHOT_COLD_SEPARATE=ON"
  "-DGC_SHORTCUT=ON"
  "-DBATCH_COMPACTION=ON"
)

FILTER="--benchmark_filter=/(80)/.*/threads:(12)$"
SKEW="true" # true (Zipfian), false (uniform)

NUMA_AFFINITY=0

mkdir -p ../results
mkdir -p ../build
cd ../build

OUTPUT_FILE=../results/breakdown_$1
TMP_OUTPUT=../results/breakdown_$1_tmp
# clean the result file
cat /dev/null > ${OUTPUT_FILE}

# disable cpu scaling
sudo cpupower frequency-set --governor performance > /dev/null

# it may take long to get third-party dependencies, so don't delete _deps
ls | grep -v _deps | xargs rm -rf
for opt in "${OPT[@]}"; do
  PACMAN_OPT="${PACMAN_OPT} ${opt}"
  echo | tee -a ${OUTPUT_FILE}
  echo ${PACMAN_OPT} | tee -a ${OUTPUT_FILE}
  # build
  cmake -DCMAKE_BUILD_TYPE=Release -DUSE_NUMA_NODE=${NUMA_AFFINITY} \
    -DINDEX_TYPE=${INDEX_TYPE} ${IDX_PERSISTENT} ${PACMAN_OPT} \
    -DNUM_KEYS=200000000 -DNUM_OPS_PER_THREAD=20000000 \
    -DNUM_WARMUP_OPS_PER_THREAD=20000000 -DSKEW=${SKEW} \
    -DNUM_GC_THREADS=2 -DYCSB_TYPE=YCSB_W100 ..

  make pacman_bench -j
  # clean cache
  sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

  numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
    ./benchmarks/pacman_bench --benchmark_repetitions=1 ${FILTER} \
    --benchmark_out=${TMP_OUTPUT} --benchmark_out_format=json
  cat ${TMP_OUTPUT} >> ${OUTPUT_FILE}

  sleep 5s
done
rm ${TMP_OUTPUT}

sudo cpupower frequency-set --governor powersave > /dev/null
