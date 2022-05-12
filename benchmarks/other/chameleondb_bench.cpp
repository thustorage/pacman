#include "benchmarks/bench_base.h"
#include "ChameleonDB/chameleon_db.h"
#include "util/index_arena.h"

#include <filesystem>

using namespace CHAMELEONDB_NAMESPACE;

thread_local std::unique_ptr<ChameleonDB::Worker> worker;

class ChameleonDBFixture : public BaseFixture {
 public:
  enum FixtureArg { Arg_init_util };
  ChameleonDBFixture() {}

 protected:
  ChameleonDB *db_ = nullptr;

  virtual void OpenDB(benchmark::State &st) override {
    if (st.thread_index() == 0) {
      if (db_ != nullptr) {
        ERROR_EXIT("barrier error");
      }
      const int num_threads = st.threads();
      const int init_util = st.range(Arg_init_util);
#ifdef USE_ALL_CORES
      int num_gc_threads = NUM_ALL_CORES - num_threads;
#else
      int num_gc_threads = NUM_GC_THREADS;
#endif
      printf("threads of service / gc : %d / %d\n", num_threads,
             num_gc_threads);
      
      double avg_val_size = VALUE_SIZE;
      if constexpr (benchmark_workload == ETC) {
        avg_val_size = ETC_AVG_VALUE_SIZE;
      }
      double object_size = sizeof(KVItem) + sizeof(KeyType) + avg_val_size;
      uint64_t total_size = 0;
      if (init_util > 0) {
        double init_size = object_size * NUM_KEYS * SEGMENT_SIZE /
                           LogSegment::SEGMENT_DATA_SIZE;
        total_size = init_size * 100. / init_util;
        total_size =
            (total_size + SEGMENT_SIZE - 1) / SEGMENT_SIZE * SEGMENT_SIZE;
        if (total_size < init_size + num_threads * 3 * SEGMENT_SIZE) {
          printf("Warning: not enough space for free segment per thread\n");
          total_size = init_size + num_threads * 3 * SEGMENT_SIZE;
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
            total_put_ops * object_size + num_threads * SEGMENT_SIZE * 2;
        num_gc_threads = 0;
      }
      std::string db_path = std::string(PMEM_DIR) + "chameleondb";
      std::filesystem::remove_all(db_path);
      std::filesystem::create_directory(db_path);

      db_ = new ChameleonDB(db_path, total_size, num_threads, num_gc_threads);
    }
    barrier.Wait(st.threads());

    worker = db_->GetWorker();
  }

  virtual void CloseDB(benchmark::State &st) override {
    worker.reset();
    barrier.Wait(st.threads());
    if (st.thread_index() == 0) {
      delete db_;
      db_ = nullptr;
    }
  }

  virtual bool Get(const Slice &key, std::string *value) override {
    return worker->Get(key, value);
  }

  virtual void Put(const Slice &key, const Slice &value) override {
    worker->Put(key, value);
  }

  virtual void PreSetUp(benchmark::State &st) override {
    // bind_core_on_numa(st.thread_index());
  }
};

BENCHMARK_DEFINE_F(ChameleonDBFixture, bench)(benchmark::State &st) {
  for (auto _ : st) {
    RunWorkload(st);
  }
  assert(st.iterations() == 1);
  st.SetItemsProcessed(st.iterations() * actual_num_ops_per_thread);
  if (st.thread_index() == 0) {
#ifdef MEASURE_LATENCY
    for (int i = 0; i < TypeEnumMax; i++) {
      HistogramData hist_data;
      for (int j = 1; j < st.threads(); j++) {
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

BENCHMARK_REGISTER_F(ChameleonDBFixture, bench)
    ->Arg(0)
    ->DenseRange(20, 90, 10)
    ->DenseThreadRange(1, 32, 1)
    ->Iterations(1)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK_MAIN();
