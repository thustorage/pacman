cmake_minimum_required(VERSION 3.14)
project(PACMAN) 
set(CMAKE_CXX_STANDARD 17)

# configurations

# config.h

set(USE_NUMA_NODE 0 CACHE STRING "To use which numa node")
option(IDX_PERSISTENT "IDX_PERSISTENT" ON)
option(LOG_PERSISTENT "LOG_PERSISTENT" ON)
set(INDEX_TYPE 1 CACHE STRING "Index type of DB") # 1 CCEH 2 FastFair 3 Masstree
if (INDEX_TYPE EQUAL 3) # MASSTREE
  set(IDX_PERSISTENT OFF)
endif()

option(LOG_BATCHING "LOG_BATCHING to simulate FlatStore's batching (if LOG_PERSISTENT)" ON)

option(REDUCE_PM_ACCESS "REDUCE_PM_ACCESS" OFF)
option(HOT_COLD_SEPARATE "HOT_COLD_SEPARATE" OFF)
option(GC_SHORTCUT "GC_SHORTCUT" OFF)
option(BATCH_COMPACTION "BATCH_COMPACTION" OFF)

option(PACMAN "PACMAN" OFF)
if (PACMAN)
  set(REDUCE_PM_ACCESS ON)
  set(HOT_COLD_SEPARATE ON)
  set(BATCH_COMPACTION ON)
  if (INDEX_TYPE EQUAL 1 AND NOT IDX_PERSISTENT)
    set(GC_SHORTCUT OFF)
  else()
    set(GC_SHORTCUT ON)
  endif()
endif()

message(STATUS "SET USE_NUMA_NODE ${USE_NUMA_NODE}")
message(STATUS "SET INDEX_TYPE ${INDEX_TYPE}")
message(STATUS "SET IDX_PERSISTENT ${IDX_PERSISTENT}")
message(STATUS "SET LOG_PERSISTENT ${LOG_PERSISTENT}")
message(STATUS "SET REDUCE_PM_ACCESS ${REDUCE_PM_ACCESS}")
message(STATUS "SET HOT_COLD_SEPARATE ${HOT_COLD_SEPARATE}")
message(STATUS "SET GC_SHORTCUT ${GC_SHORTCUT}")
message(STATUS "SET BATCH_COMPACTION ${BATCH_COMPACTION}")

configure_file(
  "${PROJECT_SOURCE_DIR}/include/config.h.in"
  "${PROJECT_BINARY_DIR}/config.h"
)

# bench_config.h

set(NUM_KEYS 200000000 CACHE STRING "number of keys")
set(NUM_OPS_PER_THREAD 20000000 CACHE STRING "number of operations per thread")
set(NUM_WARMUP_OPS_PER_THREAD 0 CACHE STRING "number of warmup operations per thread")
set(VALUE_SIZE 48 CACHE STRING "value size")
set(NUM_GC_THREADS 4 CACHE STRING "number of cleaner threads")
set(WORKLOAD_TYPE YCSB CACHE STRING "workload type")  # YCSB ETC
set(YCSB_TYPE YCSB_A CACHE STRING "ycsb workload type if using YCSB")
# YCSB_A, YCSB_B, YCSB_C, YCSB_E, YCSB_W0, YCSB_W20, YCSB_W40, YCSB_W60, YCSB_W80, YCSB_W100
set(SKEW true CACHE STRING "Zipfian (skew) workload") # true false

option(MEASURE_LATENCY "MEASURE_LATENCY" OFF)
option(USE_ALL_CORES "use all cores in a numa node" OFF)
option(TEST_LOAD "TEST_LOAD" OFF)

configure_file(
  "${PROJECT_SOURCE_DIR}/benchmarks/bench_config.h.in"
  "${PROJECT_BINARY_DIR}/bench_config.h"
)

############

option(EVAL_OTHER_SYSTEMS "build and evaluate other systems" ON)
if (EVAL_OTHER_SYSTEMS)
  message(STATUS "Will build and evaluate other systems")
endif()

option(LOGGING "LOGGING" OFF)

if (CMAKE_BUILD_TYPE MATCHES Release)
  message(STATUS "Setting build type to 'Release'")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb")
  set(LOGGING ON)
endif()

if (LOGGING)
  add_compile_definitions(LOGGING)
  message(STATUS "enable LOGGING")
endif()

#################################################################

find_library(PMEM pmem)
find_library(PMEMOBJ pmemobj)

include_directories(
  "${PROJECT_SOURCE_DIR}/include"
  "${PROJECT_SOURCE_DIR}"
  "${PROJECT_BINARY_DIR}"
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -march=native")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -g -ggdb -pthread -march=native")

aux_source_directory(${PROJECT_SOURCE_DIR}/db DB_FILES)
aux_source_directory(${PROJECT_SOURCE_DIR}/util UTIL_FILES)
set(DB_FILES ${DB_FILES} ${UTIL_FILES})

# FastFair
add_library(fastfair ${PROJECT_SOURCE_DIR}/db/index/FAST_FAIR/ff_btree.cpp)

# CCEH
add_library(cceh ${PROJECT_SOURCE_DIR}/db/index/CCEH/CCEH_MSB.cpp)

# Masstree
aux_source_directory(${PROJECT_SOURCE_DIR}/db/index/masstree MASSTREE_SOURCE_FILES)
add_library(masstree ${MASSTREE_SOURCE_FILES})
# target_include_directories(masstree ${PROJECT_SOURCE_DIR}/db/index/masstree)
target_compile_options(masstree PRIVATE
          $<$<COMPILE_LANGUAGE:CXX>:-include ${PROJECT_SOURCE_DIR}/db/index/masstree/config.h>)

add_subdirectory(benchmarks)
add_subdirectory(example)
