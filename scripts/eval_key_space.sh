#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

help() {
  echo "Usage: $0 <db_type> <apply_pacman>"
  echo "  <db_type>: 1: FlatStore-H, 2: FlatStore-PH, 3: FlatStore-FF, 4: FlatStore-M, 5: Viper"
  echo "  <apply_pacman>: 0: false, 1: true"
}

if [[ $# != 2 || $1 < 1 || $1 > 5 || ($2 != 0 && $2 != 1) ]]; then
  help
  exit
fi

# to avoid no available space
./clean_pmem_dir.sh

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

if [[ $1 != 5 ]]; then
  TARGET="pacman_bench"
  TARGET_CMD="./benchmarks/pacman_bench"
else
  TARGET="viper_bench"
  TARGET_CMD="./benchmarks/other/viper_bench"
  WITH_OTHERS="-DEVAL_OTHER_SYSTEMS=ON"
fi

PACMAN_OPT=""
if [[ $2 == 1 ]]; then
  PACMAN_OPT="-DPACMAN=ON"
fi

FILTER="--benchmark_filter=/(80)/.*/threads:(12)$"
SKEW="true" # true (Zipfian), false (uniform)

NUMA_AFFINITY=0

mkdir -p ../results
mkdir -p ../build
cd ../build

OUTPUT_FILE=../results/key_space_$1_$2
TMP_OUTPUT=../results/key_space_$1_$2_tmp
# clean the result file
cat /dev/null > ${OUTPUT_FILE}

# disable cpu scaling
sudo cpupower frequency-set --governor performance > /dev/null

NUM_KEYS=(
  50000000
  100000000
  200000000
  400000000
  800000000
)

# it may take long to get third-party dependencies, so don't delete _deps
ls | grep -v _deps | xargs rm -rf
for num in "${NUM_KEYS[@]}"; do
  echo | tee -a ${OUTPUT_FILE}
  echo ${num} | tee -a ${OUTPUT_FILE}
  num_warmup=$((${num} / 8))
  # build
  cmake -DCMAKE_BUILD_TYPE=Release -DUSE_NUMA_NODE=${NUMA_AFFINITY} \
    ${WITH_OTHERS} -DINDEX_TYPE=${INDEX_TYPE} ${IDX_PERSISTENT} ${PACMAN_OPT} \
    -DNUM_KEYS=${num} -DNUM_OPS_PER_THREAD=40000000 \
    -DNUM_WARMUP_OPS_PER_THREAD=${num_warmup} -DNUM_GC_THREADS=4 \
    -DSKEW=${SKEW} ..

  make ${TARGET} -j
  # clean cache
  sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

  numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
    ${TARGET_CMD} --benchmark_repetitions=1 ${FILTER} \
    --benchmark_out=${TMP_OUTPUT} --benchmark_out_format=json
  cat ${TMP_OUTPUT} >> ${OUTPUT_FILE}

  sleep 5s
done
rm ${TMP_OUTPUT}

sudo cpupower frequency-set --governor powersave > /dev/null
