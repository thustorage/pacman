#pragma once

#include "util/debug_helper.h"
// #include "config.h"
#include <cstdint>
// #include <unistd.h>
// #include <sched.h>
// #include <pthread.h>

static __attribute__((always_inline)) inline void compiler_barrier() {
  asm volatile("" ::: "memory");
}

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

// A macro to disallow the copy constructor and operator= functions
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName)                                     \
  TypeName(const TypeName &) = delete;                                         \
  TypeName &operator=(const TypeName &) = delete;
#endif


// // bind core
// static inline void bind_core(uint16_t core_id) {
//   cpu_set_t cpuset;
//   CPU_ZERO(&cpuset);
//   CPU_SET(core_id, &cpuset);
//   int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
//   if (rc != 0) {
//     ERROR_EXIT("can't bind core %d!", core_id);
//   }
// }

// // bind core on the same numa
// static inline void bind_core_on_numa(uint16_t core_seq) {
//   uint16_t core_id;
//   if (core_seq < CORES_PER_SOCKET) {
//     core_id = CPU_BIND_BEGIN_CORE + core_seq;
//   } else {
//     if (core_seq > CORES_PER_SOCKET * 2) {
//       ERROR_EXIT("core seq %d is out of range", core_seq);
//     }
//     core_id = CPU_BIND_BEGIN_CORE + NUMA_SPAN + core_seq - CORES_PER_SOCKET;
//   }
//   bind_core(core_id);
// }
