#include "benchmarks/bench_base.h"
#include "config.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"

#include <iostream>
#include <filesystem>

using namespace ROCKSDB_NAMESPACE;

class PMEMRocksDBFixture : public BaseFixture {
 protected:
  virtual void OpenDB(benchmark::State &st) override {
    if (st.thread_index() == 0) {
      failed_cnt_ = 0;
      if (db != nullptr) {
        ERROR_EXIT("barrier error");
      }

      Options options;
#ifdef ON_DCPMM
      options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());
      options.dcpmm_kvs_enable = false;
      options.dcpmm_compress_value = false;
      options.allow_mmap_reads = true;
      options.allow_mmap_writes = true;
      options.allow_dcpmm_writes = true;
      options.recycle_dcpmm_sst = true;
      printf("enable ON_DCPMM\n");
#endif
      rocksdb::BlockBasedTableOptions bbto;
      bbto.cache_index_and_filter_blocks_for_mmap_read = true;
      bbto.block_size = 256;
      bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
      options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
      const int num_threads = st.threads();
#ifdef USE_ALL_CORES
      int num_gc_threads = NUM_ALL_CORES - num_threads;
#else
      int num_gc_threads = NUM_GC_THREADS;
#endif
      printf("threads of service / gc : %d / %d\n", num_threads,
             num_gc_threads);
      options.max_background_jobs = num_gc_threads;
      options.allow_concurrent_memtable_write = true;
      options.enable_pipelined_write = true;
      options.compression = kNoCompression;
      options.create_if_missing = true;
      options.error_if_exists = true;
      // options.statistics = rocksdb::CreateDBStatistics();

      std::string db_path = std::string(PMEM_DIR) + "pmem_rocksdb";
      std::filesystem::remove_all(db_path);
      std::filesystem::create_directory(db_path);
      Status s = DB::Open(options, db_path, &db);
      if (!s.ok()) {
        ERROR_EXIT("open failed: %s\n", s.ToString().c_str());
      }
    }
  }

  virtual void CloseDB(benchmark::State &st) override {
    if (st.thread_index() == 0) {
      if (failed_cnt_ > 0) {
        printf("put failed count: %ld\n", failed_cnt_.load());
      }
      delete db;
      db = nullptr;
    }
  }

  virtual bool Get(const ::Slice &key, std::string *value) override {
    Status s =
        db->Get(ReadOptions(), rocksdb::Slice(key.data(), key.size()), value);
    return s.ok();
  }

  virtual void Put(const ::Slice &key, const ::Slice &value) override {
    Status s = db->Put(WriteOptions(), rocksdb::Slice(key.data(), key.size()),
                       rocksdb::Slice(value.data(), value.size()));
    if (!s.ok()) {
      ERROR_EXIT("put failed");
      // failed_cnt_++;
    }
  }

  // virtual void PreSetUp(benchmark::State &st) override {
  //   // don't bind core, otherwise background threads also bind
  //   // bind_core_on_numa(st.thread_index());
  // }

  // virtual void PreTearDown(benchmark::State &st) override {
  //   if (st.thread_index() == 0) {
  //     std::string stat;
  //     db->GetProperty("rocksdb.stats", &stat);
  //     std::cout << stat << std::endl;

  //     if (db->GetProperty("rocksdb.options-statistics", &stat)) {
  //       std::cout << "options-statistics" << std::endl;
  //       std::cout << stat << std::endl;
  //     } else {
  //       std::cout << "statistics is nullptr" << std::endl;
  //     }
  //   }
  // }

 private:
  DB *db = nullptr;
  std::atomic_int_fast64_t failed_cnt_{0};
};


BENCHMARK_DEFINE_F(PMEMRocksDBFixture, bench)(benchmark::State &st) {
  for (auto _ : st) {
    RunWorkload(st);
  }
  assert(st.iterations() == 1);
  st.SetItemsProcessed(st.iterations() * actual_num_ops_per_thread);
#ifdef MEASURE_LATENCY
  if (st.thread_index() == 0) {
    for (int i = 0; i < TypeEnumMax; i++) {
      ::HistogramData hist_data;
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
  }
#endif
}

BENCHMARK_REGISTER_F(PMEMRocksDBFixture, bench)
    ->DenseThreadRange(1, 32, 1)
    ->Iterations(1)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK_MAIN();
