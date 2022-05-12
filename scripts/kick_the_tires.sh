#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

# to avoid no available space
./clean_pmem_dir.sh

mkdir -p ../results
mkdir -p ../build
cd ../build

THREADS=4
NUMA_AFFINITY=0
FILTER="--benchmark_filter=/(80)/.*/threads:(${THREADS})$"

# disable cpu scaling
sudo cpupower frequency-set --governor performance > /dev/null

# it may take long to get third-party dependencies, so don't delete _deps
ls | grep -v _deps | xargs rm -rf

for i in $(seq 1 3); do
  INDEX_TYPE=${i}
  TARGET="pacman_bench"
  TARGET_CMD="./benchmarks/pacman_bench"

  # build
  cmake -DCMAKE_BUILD_TYPE=Release -DUSE_NUMA_NODE=${NUMA_AFFINITY} \
    -DEVAL_OTHER_SYSTEMS=ON -DINDEX_TYPE=${INDEX_TYPE} \
    -DIDX_PERSISTENT=OFF -DPACMAN=ON \
    -DNUM_KEYS=10000 -DNUM_OPS_PER_THREAD=10000 \
    -DNUM_GC_THREADS=2 -DWORKLOAD_TYPE=ETC -DMEASURE_LATENCY=ON ..
  make ${TARGET} -j


  numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
    ${TARGET_CMD} --benchmark_repetitions=1 ${FILTER}
done

# evaluate other systems
# viper, ChameleonDB
OTHER_SYSTEMS=(
  "viper_bench"
  "chameleondb_bench"
)

FILTER="--benchmark_filter=/(80)/.*/threads:(${THREADS})$"

cmake -DCMAKE_BUILD_TYPE=Release -DUSE_NUMA_NODE=${NUMA_AFFINITY} \
  -DEVAL_OTHER_SYSTEMS=ON -DINDEX_TYPE=1 -DIDX_PERSISTENT=OFF -DPACMAN=ON \
  -DNUM_KEYS=10000 -DNUM_OPS_PER_THREAD=10000 \
  -DNUM_GC_THREADS=2 -DWORKLOAD_TYPE=YCSB -DMEASURE_LATENCY=ON ..

for sys in "${OTHER_SYSTEMS[@]}"; do
  make ${sys} -j
  TARGET_CMD="./benchmarks/other/${sys}"
  numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
    ${TARGET_CMD} --benchmark_repetitions=1 ${FILTER}
done

# pmem_rocksdb, pmemkv
OTHER_SYSTEMS=(
  "pmem_rocksdb_bench"
  "pmemkv_bench"
)
FILTER="--benchmark_filter=/.*/threads:(${THREADS})$"

cmake -DCMAKE_BUILD_TYPE=Release -DUSE_NUMA_NODE=${NUMA_AFFINITY} \
  -DEVAL_OTHER_SYSTEMS=ON -DINDEX_TYPE=1 -DIDX_PERSISTENT=ON \
  -DNUM_KEYS=10000 -DNUM_OPS_PER_THREAD=10000 \
  -DNUM_GC_THREADS=2 -DWORKLOAD_TYPE=ETC -DMEASURE_LATENCY=ON ..

for sys in "${OTHER_SYSTEMS[@]}"; do
  make ${sys} -j
  TARGET_CMD="./benchmarks/other/${sys}"
  numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
    ${TARGET_CMD} --benchmark_repetitions=1 ${FILTER}
done


sudo cpupower frequency-set --governor powersave > /dev/null

echo
echo "Kick-the-tires passed!"
