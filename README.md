# Pacman

This repository contains the artifact for the USENIX ATC'22 paper: ["Pacman: An Efficient Compaction Approach for Log-Structured Key-Value Store on Persistent Memory"](https://www.usenix.org/conference/atc22/presentation/wang-jing).


## Directory structure

```
pacman
|---- benchmarks        # code of the benchmarks and other systems (Viper, ChameleonDB, PMem-RocksDB, and pmemkv)
|---- include           # public headers of pacman db
|---- db                # source code of pacman with PM-based log-structured KV store
|---- util              # utilities used for programming
|---- example           # a simple example of pacman db
|---- scripts           # main evaluation scripts
```

## Functionality of code
* `include/db.h, db/db.cpp`: DB interface
* `db/index/*`: indexes used by DB, modified to utilize `shortcut`

* `db/segment.h`: structure and basic functionality of log segment
* `db/log_structured.[h|cpp]`: management of log segments in DB
* `db/loc_cleaner.[h|cpp]`: main functionality of segment compaction


The major parts of Pacman's techniques are enabled by macro `GC_SHORTCUT` (Sec 3.1), `REDUCE_PM_ACCESS` (Sec 3.2), `BATCH_COMPACTION` (Sec 3.3), and `HOT_COLD_SEPARATE` (Sec 3.4). Check out the source code for more details.

## Dependencies

* libraries
  - [PMDK](https://github.com/pmem/pmdk) (libpmem & libpmemobj)
  - jemalloc

* utilities for experiments:
  - numactl
  - cpupower

If you're going to evaluate other systems (e.g., [PMem-RocksDB](https://github.com/pmem/pmem-rocksdb), [pmemkv](https://github.com/pmem/pmemkv)), please install the their dependencies. You don't need to install other systems manually since the cmake in this repository will fetch and build them automatically.

## System configuration

```shell
1. set Optane DCPMM to AppDirect mode
$ sudo ipmctl create -f -goal persistentmemorytype=appdirect

2. configure PM device to fsdax mode
$ sudo ndctl create-namespace -m fsdax

3. create and mount a file system with DAX
$ sudo mkfs.ext4 -f /dev/pmem0
$ sudo mount -o dax /dev/pmem0 /mnt/pmem0
```


## Building the benchmarks

**We recommend that you use the scripts directly from the [scripts](scripts) directly. The scripts will build and run experiments automatically.**

Quick start:

```shell
$ mkdir -p build && cd build
$ cmake -DCMAKE_BUILD_TYPE=Release -DINDEX_TYPE=1 -IDX_PERSISTENT=OFF -DPACMAN=ON .. 
$ cmake --build .
```

This will build benchmark for FlatStore-H with Pacman. You may need to set the path to pmem (`PMEM_DIR`) in the `include/config.h.in`.

[PMem-RocksDB](https://github.com/starkwj/pmem-rocksdb) and [pmemkv](https://github.com/pmem/pmemkv) will be downloaded and built by default. If you don't want to evaluate with them, pass `-DEVAL_OTHER_SYSTEMS=OFF` to cmake.


To facilitate evaluation, we use cmake to set most configurations for both systems and benchmarks. Please check out the [CMakeLists.txt](CMakeLists.txt) for more details.


## Running experiments


```
scripts
|---- kick_the_tires.sh     # script of "Hello world"-sized examples
|---- eval_utilization.sh   # impact of capacity utilization (Sec 4.2.1)
|---- eval_ycsb.sh          # YCSB benchmarks (Sec 4.2.2)
|---- eval_threads.sh       # thread scalability (Sec 4.2.3)
|---- eval_value_size.sh    # different value sizes (Sec 4.2.3)
|---- eval_write_ratio.sh   # different write ratios (Sec 4.2.3)
|---- eval_key_space.sh     # different key spaces (Sec 4.2.3)
|---- eval_breakdown.sh     # breakdown of techniques in Pacman (Sec 4.3)
|---- eval_etc.sh           # Facebook ETC workload (Sec 4.4)
|---- eval_recovery.sh      # recovery test (Sec 4.5)
|---- eval_case.sh          # for evaluating other cases with customized configurations
```

### Usage

* `kick_the_tires.sh` and `eval_case.sh`: Execute directly with no arguments.

* Usage for other scripts is:

  ```shell
  $ ./script_name.sh <db_type> # for eval_breakdown.sh and eval_recovery.sh
  $ ./script_name.sh <db_type> <apply_pacman> # for the others
  ```

  Please check out the `help` information at the top of the scripts for details.

These scripts should be run inside the `scripts` directory. Each experiment will be run for several minutes. Evaluation on PMem-RocksDB and pmemkv will take much longer (PMem-RocksDB takes about 20 minutes on the Facebook ETC workload and may even abort probably).
