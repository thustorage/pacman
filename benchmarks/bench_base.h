#pragma once

#include "bench_config.h"
#include "slice.h"
#include "zipf.h"
#include "murmur_hash2.h"
#ifdef MEASURE_LATENCY
#include "histogram.h"
#define MEASURE_SAMPLE 32
#endif

#include <benchmark/benchmark.h>
#include <atomic>
#include <random>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <unistd.h>

// key, value in Index
using KeyType = uint64_t;
using ValueType = uint64_t;

static inline uint64_t getKey(uint64_t k) {
  return MurmurHash64A(&k, sizeof(uint64_t));
}

class BaseFixture : public benchmark::Fixture {
  class Barrier {
   public:
    void Wait(int total_count) {
      std::unique_lock<std::mutex> lock(mu_);
      int local_gen = gen_;
      if (++cur_count_ == total_count) {
        // the last one
        ++gen_;
        cur_count_ = 0;
        cond_.notify_all();
      } else {
        cond_.wait(lock, [this, local_gen] { return local_gen != gen_; });
      }
    }

   private:
    std::mutex mu_;
    std::condition_variable cond_;
    volatile int cur_count_ = 0;
    volatile int gen_ = 0;
  };

 public:
  BaseFixture() {
    printf(
        "total keys: %lu NUM_OPS_PER_THREAD: %lu NUM_WARMUP_OPS_PER_THREAD: "
        "%lu\n",
        NUM_KEYS, NUM_OPS_PER_THREAD, NUM_WARMUP_OPS_PER_THREAD);
    std::string benchmark_name;
    if constexpr (benchmark_workload == YCSB) {
      printf("benchmark: %s\n", ycsb_name[ycsb_type]);
      printf("VALUE_SIZE: %lu\n", VALUE_SIZE);
      printf("distribution: %s\n", skew ? "Zipf" : "uniform");
    } else {
      printf("benchmark: ETC\n");
    }
    keys_seq.resize(NUM_KEYS);
    for (size_t i = 0; i < NUM_KEYS; i++) {
      keys_seq[i] = i;
    }
    std::shuffle(keys_seq.begin(), keys_seq.end(),
                 std::default_random_engine(1234));
  }

  virtual void SetUp(benchmark::State &st) override final {
    PreSetUp(st);

    const int num_threads = st.threads();
    if (st.thread_index() == 0) {
#ifdef TEST_LOAD
      actual_num_ops_per_thread = NUM_KEYS / num_threads;
#else
      actual_num_ops_per_thread = NUM_OPS_PER_THREAD;
#endif
      if constexpr (benchmark_workload == YCSB) {
        workload_key_bases =
            std::make_unique<std::vector<uint64_t>[]>(num_threads);
      }
#ifdef MEASURE_LATENCY
      latency_statistics = std::make_unique<LatencyStatistics[]>(num_threads);
#endif
    }
    OpenDB(st);
    barrier.Wait(st.threads());

#ifndef TEST_LOAD
    // prefill
    Load(st);

    if constexpr (benchmark_workload == YCSB) {
      // generate workload keys
      workload_key_bases[st.thread_index()].reserve(actual_num_ops_per_thread);
      Random rand(st.thread_index() + 128);
      zipf_gen_state zipf_state;
      mehcached_zipf_init(&zipf_state, NUM_KEYS, ZIPF_THETA,
                          st.thread_index() + 128);
      for (size_t i = 0; i < actual_num_ops_per_thread; ++i) {
        uint64_t k;
        if (skew) {
          k = getKey(mehcached_zipf_next(&zipf_state));
        } else {
          k = getKey(rand.Next() % NUM_KEYS);
        }
        workload_key_bases[st.thread_index()].push_back(k);
      }
    }
#endif

    barrier.Wait(st.threads());
    if (NUM_WARMUP_OPS_PER_THREAD > 0) {
      // warm up
      if constexpr (benchmark_workload == YCSB) {
        Random rand(st.thread_index() + 256);
        zipf_gen_state zipf_state;
        mehcached_zipf_init(&zipf_state, NUM_KEYS, ZIPF_THETA,
                            st.thread_index() + 256);
        std::vector<uint64_t> warm_up_key_bases;
        warm_up_key_bases.reserve(NUM_WARMUP_OPS_PER_THREAD);
        for (size_t i = 0; i < NUM_WARMUP_OPS_PER_THREAD; i++) {
          uint64_t key;
          if (skew) {
            key = getKey(mehcached_zipf_next(&zipf_state));
          } else {
            key = getKey(rand.Uniform(NUM_KEYS));
          }
          warm_up_key_bases.push_back(key);
        }
        RunYCSBWorkload(st, 2048, warm_up_key_bases);
      } else {
        RunETCWorkload(st, 2048);
      }
#ifdef MEASURE_LATENCY
      latency_statistics[st.thread_index()].Clear();  // clear warmup statistics
#endif
      barrier.Wait(st.threads());
    }
  }

  virtual void TearDown(benchmark::State &st) override {
    CloseDB(st);
    if (st.thread_index() == 0) {
      workload_key_bases.reset();
    }
    sleep(5);
  }

  virtual void Load(benchmark::State &st) {
    const int num_threads = st.threads();
    size_t num_load_per_thread = NUM_KEYS / num_threads;
    if (NUM_KEYS % num_threads) {
      ++num_load_per_thread;
    }
    size_t load_begin = st.thread_index() * num_load_per_thread;
    size_t load_end = std::min(load_begin + num_load_per_thread, NUM_KEYS);

    Random rand(st.thread_index() + 1024);
    char buf[4096];

    for (size_t i = load_begin; i < load_end; ++i) {
      size_t val_size = VALUE_SIZE;
      size_t seq = keys_seq[i];
      if constexpr (benchmark_workload == ETC) {
        if (seq < ETC_medium_seq_boundary) {
          val_size = ETC_get_value_size(rand, ETC_Kind::small);
        } else if (seq < ETC_large_seq_boundary) {
          val_size = ETC_get_value_size(rand, ETC_Kind::medium);
        } else {
          val_size = ETC_get_value_size(rand, ETC_Kind::large);
        }
      }
      KeyType key = getKey(seq);
      memcpy(buf, &key, sizeof(KeyType));
      Put(Slice((const char *)&key, sizeof(KeyType)), Slice(buf, val_size));
    }
  }

  virtual void RunYCSBWorkload(benchmark::State &st, int rand_seed,
                               std::vector<uint64_t> &key_bases) {
    Random rand(st.thread_index() + rand_seed);
    char buf[VALUE_SIZE];
    const size_t num_ops = key_bases.size();
    for (size_t i = 0; i < num_ops; i++) {
      OP_Type op = get_op_type(&rand, ycsb_type);
      KeyType key = key_bases[i];
      switch (op) {
        case OP_Read: {
#ifdef MEASURE_LATENCY
          Histogram *sampled_hist =
              i % MEASURE_SAMPLE == 0
                  ? &latency_statistics[st.thread_index()].histograms[TypeGet]
                  : nullptr;
          StopWatch sw(sampled_hist);
#endif
          std::string value;
          bool found = Get(Slice((const char *)&key, sizeof(KeyType)), &value);
          break;
        }
        case OP_Insert:
        case OP_Update: {
#ifdef MEASURE_LATENCY
          Histogram *sampled_hist =
              i % MEASURE_SAMPLE == 0
                  ? &latency_statistics[st.thread_index()].histograms[TypePut]
                  : nullptr;
          StopWatch sw(sampled_hist);
#endif
          memcpy(buf, &key, sizeof(KeyType));
          Put(Slice((const char *)&key, sizeof(KeyType)),
              Slice(buf, VALUE_SIZE));
          break;
        }
        case OP_Scan: {
          size_t scan_length = rand.Next() % MAX_SCAN_LENGTH + 1;
          Scan(Slice((const char *)&key, sizeof(KeyType)), scan_length);
          break;
        }
        default:
          ERROR_EXIT("error operation type %d\n", op);
          break;
      }
    }
  }

  virtual void RunETCWorkload(benchmark::State &st, int rand_seed) {
    Random rand(st.thread_index() + rand_seed);
    zipf_gen_state zipf_state_small;
    zipf_gen_state zipf_state_medium;
    mehcached_zipf_init(&zipf_state_small, ETC_medium_seq_boundary, ZIPF_THETA,
                        st.thread_index() + rand_seed);
    mehcached_zipf_init(&zipf_state_medium,
                        ETC_large_seq_boundary - ETC_medium_seq_boundary,
                        ZIPF_THETA, st.thread_index() + 1024 + rand_seed);
    char buf[4096];
    for (size_t i = 0; i < actual_num_ops_per_thread; i++) {
      ETC_Kind kind = ETC_get_kind(rand);
      uint64_t key_base;
      if (kind == ETC_Kind::small) {
        key_base = getKey(mehcached_zipf_next(&zipf_state_small));
      } else if (kind == ETC_Kind::medium) {
        key_base = getKey(ETC_medium_seq_boundary +
                          mehcached_zipf_next(&zipf_state_medium));
      } else {
        key_base = getKey(ETC_large_seq_boundary +
                          rand.Uniform(NUM_KEYS - ETC_large_seq_boundary));
      }
      KeyType key = key_base;

      if (get_op_type(&rand, YCSB_A) == OP_Read) {
#ifdef MEASURE_LATENCY
        Histogram *sampled_hist =
            i % MEASURE_SAMPLE == 0
                ? &latency_statistics[st.thread_index()].histograms[TypeGet]
                : nullptr;
        StopWatch sw(sampled_hist);
#endif
        std::string value;
        bool found = Get(Slice((const char *)&key, sizeof(KeyType)), &value);
      } else {
#ifdef MEASURE_LATENCY
        Histogram *sampled_hist =
            i % MEASURE_SAMPLE == 0
                ? &latency_statistics[st.thread_index()].histograms[TypePut]
                : nullptr;
        StopWatch sw(sampled_hist);
#endif
        size_t val_size = ETC_get_value_size(rand, kind);
        memcpy(buf, &key, sizeof(KeyType));
        Put(Slice((const char *)&key, sizeof(KeyType)), Slice(buf, val_size));
      }
    }
  }

  virtual void RunWorkload(benchmark::State &st) {
#ifdef TEST_LOAD
    Load(st);
#else
    if constexpr (benchmark_workload == YCSB) {
      RunYCSBWorkload(st, 512, workload_key_bases[st.thread_index()]);
    } else if constexpr (benchmark_workload == ETC) {
      RunETCWorkload(st, 512);
    }
#endif
  }

 protected:
  Barrier barrier;
  size_t actual_num_ops_per_thread = 0;
  std::vector<uint64_t> keys_seq;
  std::unique_ptr<std::vector<uint64_t>[]> workload_key_bases;

  static constexpr size_t ETC_medium_seq_boundary = NUM_KEYS * 40 / 100;
  static constexpr size_t ETC_large_seq_boundary = NUM_KEYS * 95 / 100;

#ifdef MEASURE_LATENCY
  enum StatisticType : uint8_t{TypeGet, TypePut, TypeEnumMax};
  static constexpr char *TypeStrings[] = {(char *)"Get", (char *)"Put",
                                          (char *)"EnumMax"};
  struct alignas(64) LatencyStatistics {
    Histogram histograms[TypeEnumMax];
    void Clear() {
      for (uint8_t i = 0; i < TypeEnumMax; i++) {
        histograms[i].Clear();
      }
    }
  };
  std::unique_ptr<LatencyStatistics[]> latency_statistics;
#endif

  virtual void OpenDB(benchmark::State &st) = 0;
  virtual void CloseDB(benchmark::State &st) = 0;

  virtual bool Get(const Slice &key, std::string *value) = 0;
  virtual void Put(const Slice &key, const Slice &value) = 0;
  virtual size_t Scan(const Slice &key, int scan_length) {
    ERROR_EXIT("not supported");
  }
  // delete

  virtual void PreSetUp(benchmark::State &st) {}
  virtual void PreTearDown(benchmark::State &st) {}
};
