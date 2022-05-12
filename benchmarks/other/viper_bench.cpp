#include "benchmarks/bench_base.h"
#include "benchmarks/histogram.h"
#include "viper/viper.hpp"
#include "config.h"

#include <mutex>
#include <condition_variable>

thread_local std::unique_ptr<
    viper::Viper<std::string, std::string>::ClientWrapper>
    client;

class ViperFixture : public BaseFixture {
 public:
  enum FixtureArg { Arg_init_util };

 protected:
  std::unique_ptr<viper::Viper<std::string, std::string>> db_;

  virtual void OpenDB(benchmark::State &st) override {
    if (st.thread_index() == 0) {
      if (db_) {
        ERROR_EXIT("barrier error");
      }

      const int num_threads = st.threads();
      const int init_util = st.range(Arg_init_util);

#ifdef USE_ALL_CORES
      int num_gc_threads = NUM_ALL_CORES - num_threads;
#else
      int num_gc_threads = NUM_GC_THREADS;
#endif
      constexpr int DATA_SIZE =
          viper::internal::ViperPage<std::string, std::string>::DATA_SIZE;
      constexpr int PAGE_SIZE = viper::PAGE_SIZE;
      double avg_val_size = VALUE_SIZE;
      if constexpr (benchmark_workload == ETC) {
        avg_val_size = ETC_AVG_VALUE_SIZE;
      }
      const double object_size =
          sizeof(viper::internal::VarSizeEntry::size_info) + sizeof(KeyType) +
          avg_val_size;
      uint64_t total_size = 0;
      if (init_util > 0) {
        double init_size = object_size * NUM_KEYS * PAGE_SIZE / DATA_SIZE;
        total_size = init_size * 100. / init_util;
        if (total_size < init_size + num_threads * 3 * viper::BLOCK_SIZE) {
          printf("Warning: not enough space for free segment per thread\n");
          total_size = init_size + num_threads * 3 * viper::BLOCK_SIZE;
        }
      } else {
        // infinity log space <=> no gc
        YCSB_Type type = ycsb_type;
        if constexpr (benchmark_workload == ETC) {
          type = YCSB_A;
        }
        uint64_t total_put_ops =
            NUM_KEYS + (uint64_t)actual_num_ops_per_thread *
                           (YCSB_Put_Ratio[type] + 10) / 100 * num_threads;
        total_size =
            total_put_ops * object_size + num_threads * viper::BLOCK_SIZE;
        num_gc_threads = 0;
      }
      total_size = (total_size + viper::BLOCK_SIZE - 1) / viper::BLOCK_SIZE *
                   viper::BLOCK_SIZE;
      printf("threads of service / gc : %d / %d\n", num_threads,
             num_gc_threads);

      viper::ViperConfig v_config;
      v_config.num_client_threads = num_threads;
      v_config.num_reclaim_threads = num_gc_threads;
      if (num_gc_threads == 0) {
        v_config.enable_reclamation = false;
      }
      std::string db_path = std::string(PMEM_DIR) + "viper";
      std::filesystem::remove_all(db_path);
      std::filesystem::create_directory(db_path);

#ifdef CCEH_PERSISTENT
      viper::PMemAllocator::get().initialize();
#endif
      db_ = viper::Viper<std::string, std::string>::create(db_path, total_size,
                                                           v_config);
    }

    barrier.Wait(st.threads());
    client = db_->get_client_ptr();
  }

  virtual void CloseDB(benchmark::State &st) override {
    client.reset();
    barrier.Wait(st.threads());
    if (st.thread_index() == 0) {
      db_.reset();
    }
  }

  virtual bool Get(const Slice &key, std::string *value) override {
    return client->get(std::string(key.data(), key.size()), value);
  }

  virtual void Put(const Slice &key, const Slice &value) override {
    client->put(std::string(key.data(), key.size()),
                std::string(value.data(), value.size()));
  }

  virtual void PreSetUp(benchmark::State &st) override {
    // bind_core_on_numa(st.thread_index());
  }
};


BENCHMARK_DEFINE_F(ViperFixture, bench)(benchmark::State &st) {
  if (st.thread_index() == 0) {
    db_->StartCleanStatistics();
  }
  for (auto _ : st) {
    RunWorkload(st);
  }

  assert(st.iterations() == 1);
  st.SetItemsProcessed(st.iterations() * actual_num_ops_per_thread);
  if (st.thread_index() == 0) {
    double compaction_cpu_usage = db_->GetCompactionCPUUsage();
    double compaction_tp = db_->GetReclaimThroughput();
    st.counters["CompactionCPUUsage"] = compaction_cpu_usage;
    st.counters["CompactionThroughput"] =
        benchmark::Counter(compaction_tp, benchmark::Counter::kDefaults,
                           benchmark::Counter::kIs1024);
#ifdef MEASURE_LATENCY
    for (int i = 0; i < TypeEnumMax; i++) {
      HistogramData hist_data;
      for (int j = 1; j < ((st.threads())); j++) {
        latency_statistics[0].histograms[i].Merge(
            latency_statistics[j].histograms[i]);
      }
      latency_statistics[0].histograms[i].Data(&hist_data);
      std::string name = std::string("Lat_") + TypeStrings[i] + "_";
      st.counters[name + "Avg"] = hist_data.average;
      st.counters[name + "P50"] = hist_data.median;
      st.counters[name + "P95"] = hist_data.percentile95;
      st.counters[name + "P99"] = hist_data.percentile99;
    }
    latency_statistics.reset();
#endif
  }
}

BENCHMARK_REGISTER_F(ViperFixture, bench)
    ->DenseRange(50, 90, 10)
    // ->DenseThreadRange(6, 36, 6)
    ->DenseThreadRange(1, 36, 1)
    ->Iterations(1)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK_MAIN();
