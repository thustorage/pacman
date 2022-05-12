#include "benchmarks/bench_base.h"
#include "config.h"

#include <libpmemkv.hpp>
#include <atomic>
#include <random>
#include <string_view>
#include <unistd.h>

static constexpr uint64_t pmemkv_pool_size = 128ul << 30;

enum FixtureArg { Arg_value_size };

class PMEMKVFixture : public BaseFixture {
 protected:
  virtual void OpenDB(benchmark::State &st) override {
    if (st.thread_index() == 0) {
      pmem::kv::config cfg;
      pmem::kv::status s = cfg.put_size(pmemkv_pool_size);
      assert(s == pmem::kv::status::OK);
      s = cfg.put_create_or_error_if_exists(true);
      assert(s == pmem::kv::status::OK);
      db = new pmem::kv::db();
#ifdef IDX_PERSISTENT
      std::string pool_path = std::string(PMEM_DIR) + "pmemkv_pool";
      remove(pool_path.c_str());
      s = cfg.put_path(pool_path);
      assert(s == pmem::kv::status::OK);
      s = db->open("cmap", std::move(cfg));
#else
      ERROR_EXIT("not supported");
#endif
      if (s != pmem::kv::status::OK) {
        ERROR_EXIT("pmemkv open failed");
      }
    }
  }

  virtual void CloseDB(benchmark::State &st) override {
    if (st.thread_index() == 0) {
      db->close();
      delete db;
      db = nullptr;
    }
  }

  virtual bool Get(const Slice &key, std::string *value) override {
    pmem::kv::status s =
        db->get(pmem::kv::string_view(key.data(), key.size()), value);
    return s == pmem::kv::status::OK;
  }

  virtual void Put(const Slice &key, const Slice &value) override {
    pmem::kv::status s =
        db->put(pmem::kv::string_view(key.data(), key.size()),
                pmem::kv::string_view(value.data(), value.size()));
    if (s != pmem::kv::status::OK) {
      ERROR_EXIT("put failed");
    }
  }

  virtual void PreSetUp(benchmark::State &st) override {
    // bind_core_on_numa(st.thread_index());
  }

 private:
  pmem::kv::db *db = nullptr;
};


BENCHMARK_DEFINE_F(PMEMKVFixture, bench)(benchmark::State &st) {
  for (auto _ : st) {
    RunWorkload(st);
  }
  assert(st.iterations() == 1);
  st.SetItemsProcessed(st.iterations() * actual_num_ops_per_thread);
#ifdef MEASURE_LATENCY
  if (st.thread_index() == 0) {
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
  }
#endif
}

BENCHMARK_REGISTER_F(PMEMKVFixture, bench)
    ->DenseThreadRange(1, 36, 1)
    ->Iterations(1)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK_MAIN();
