# pacman_bench
find_package(benchmark)
if (NOT benchmark_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    benchmark
    GIT_REPOSITORY  https://github.com/google/benchmark.git
    GIT_TAG         f91b6b42b1b9854772a90ae9501464a161707d1e  # v1.6.0
  )
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Suppressing benchmark's tests" FORCE)
  FetchContent_MakeAvailable(benchmark)
endif()

set(BENCHMARK_FILES
  ${PROJECT_SOURCE_DIR}/benchmarks/benchmark.cpp
  ${PROJECT_SOURCE_DIR}/benchmarks/histogram.cpp
)

add_executable(pacman_bench ${BENCHMARK_FILES} ${DB_FILES})
target_link_libraries(pacman_bench benchmark::benchmark ${PMEM} ${PMEMOBJ} jemalloc)
if (INDEX_TYPE EQUAL 1)
  target_link_libraries(pacman_bench cceh)
elseif (INDEX_TYPE EQUAL 2)
  target_link_libraries(pacman_bench fastfair)
elseif (INDEX_TYPE EQUAL 3)
  target_link_libraries(pacman_bench masstree)
endif()

# recovery_test
add_executable(recovery_test
      ${PROJECT_SOURCE_DIR}/benchmarks/recovery_test.cpp ${DB_FILES})
target_link_libraries(recovery_test ${PMEM} ${PMEMOBJ})
if (INDEX_TYPE EQUAL 1)
  target_link_libraries(recovery_test cceh)
elseif (INDEX_TYPE EQUAL 2)
  target_link_libraries(recovery_test fastfair)
elseif (INDEX_TYPE EQUAL 3)
  target_link_libraries(recovery_test masstree)
endif()

if (EVAL_OTHER_SYSTEMS)
  add_subdirectory(other)
endif()
