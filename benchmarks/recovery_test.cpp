#include <thread>
#include <string>
#include <getopt.h>
#include <random>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <filesystem>

#include "zipf.h"
#include "db.h"
#include "trace.h"
#include "murmur_hash2.h"
#include "config.h"

static inline uint64_t getKey(uint64_t k) {
  return MurmurHash64A(&k, sizeof(uint64_t));
}

static int FLAGS_num = 10000000;
static int FLAGS_num_total_ops = 1000000;
static int FLAGS_num_ops = 1000000;
int FLAGS_threads = 4;
int FLAGS_gc_threads = 1;
int FLAGS_value_size = 100;
static bool FLAGS_YCSB_zipf = 1;
uint64_t FLAGS_log_size = 20ul << 30;
int FLAGS_init_utilization = 80;
double FLAGS_zipf_theta = 0.99;

YCSB_Type ycsb_type = YCSB_W100;

struct Stats {
  double start;
  double finish;
  double seconds;
  int done;
  int next_report;
  double tput;
  bool report;

  Stats() : report(false) {}

  void Start() {
    seconds = 0;
    done = 0;
    next_report = 100;
    tput = 0;
    start = finish = NowMicros();
  }

  void SetReport() { report = true; }

  void Stop() {
    finish = NowMicros();
    seconds = (finish - start) * 1e-6;
    tput = done / seconds;
  }

  void FinishedSingleOp() {
    done++;
    if (report && done >= next_report) {
      if (next_report < 1000)
        next_report += 100;
      else if (next_report < 5000)
        next_report += 500;
      else if (next_report < 10000)
        next_report += 1000;
      else if (next_report < 50000)
        next_report += 5000;
      else if (next_report < 100000)
        next_report += 10000;
      else if (next_report < 500000)
        next_report += 50000;
      else
        next_report += 100000;
      std::fprintf(stderr, "... finished %d ops%30s\r", done, "");
      std::fflush(stderr);
    }
  }

  void Merge(const Stats &other) {
    seconds += other.seconds;
    done += other.done;
    tput += other.tput;
  }

  void Report(const Slice &name) {
    if (done < 1)
      done = 1;
    std::fprintf(stdout, "%-12s : %11.3lf micros/op; %11.1lf kops/s\n",
                 name.ToString().c_str(), seconds * 1e6 / done, tput / 1000.);
    std::fflush(stdout);
  }
};

struct SharedState {
  std::mutex mu;
  std::condition_variable cv;
  int total;
  int num_initialized;
  int num_done;
  const KeyType *keys;
  bool start;

  SharedState(int total)
      : total(total), num_initialized(0), num_done(0),
        keys(nullptr), start(false) {}
};

struct alignas(CACHE_LINE_SIZE) ThreadArg {
  SharedState *shared;
  Stats stats;
  DB *db;
  int id;
  int num_ops;
};

void thread_task(ThreadArg *t) {
  DB *db = t->db;
  int num_ops = t->num_ops;

  Random rand(t->id + 1024);
  zipf_gen_state zipf_state;
  mehcached_zipf_init(&zipf_state, FLAGS_num, FLAGS_zipf_theta, t->id + 512);
  // begin
  std::unique_ptr<DB::Worker> worker = db->GetWorker();
  KeyType k;
  for (int i = 0; i < num_ops; i++) {
    if (FLAGS_YCSB_zipf) {
      k = getKey(mehcached_zipf_next(&zipf_state));
    } else {
      k = getKey(rand.Next() % FLAGS_num);
    }
    OP_Type op = get_op_type(&rand, ycsb_type);
    switch (op) {
      case OP_Read: {
        std::string value;
        bool found =
            worker->Get(Slice((const char *)&k, sizeof(KeyType)), &value);
        break;
      }
      case OP_Insert:
      case OP_Update: {
        char buf[4096];
        snprintf(buf, sizeof(buf), "%020lu", k);
        worker->Put(Slice((const char *)&k, sizeof(KeyType)),
                    Slice(buf, FLAGS_value_size));
        break;
      }
      default:
        ERROR_EXIT("error operation type %d\n", op);
        break;
    }
    t->stats.FinishedSingleOp();
  }
}

void thread_body(ThreadArg *t) {
  // bind_core_on_numa(t->id);

  SharedState *shared = t->shared;

  // load
  const KeyType *keys = shared->keys;
  const int value_size = FLAGS_value_size;
  int num_load_per_thread = FLAGS_num / FLAGS_threads;
  if (FLAGS_num % FLAGS_threads) {
    ++num_load_per_thread;
  }
  int load_begin = t->id * num_load_per_thread;
  int load_end = std::min(load_begin + num_load_per_thread, FLAGS_num);
  t->stats.Start();
  {
    std::unique_ptr<DB::Worker> worker = t->db->GetWorker();
    for (int i = load_begin; i < load_end; i++) {
      char buf[value_size];
      uint64_t k = *(uint64_t *)(&keys[i]);
      snprintf(buf, sizeof(buf), "%020lu", (uint64_t)k);
      worker->Put(Slice((const char *)&keys[i], sizeof(KeyType)),
                  Slice(buf, value_size));
      t->stats.FinishedSingleOp();
    }
  }
  t->stats.Stop();

  {
    std::unique_lock<std::mutex> lk(shared->mu);
    shared->num_initialized++;
    if (shared->num_initialized >= shared->total) {
      shared->cv.notify_all();
    }
    while (!shared->start) {
      shared->cv.wait(lk);
    }
  }

  t->stats.Start();
  thread_task(t);
  t->stats.Stop();

  {
    std::lock_guard<std::mutex> lk(shared->mu);
    shared->num_done++;
    if (shared->num_done >= shared->total) {
      shared->cv.notify_all();
    }
  }
}


int main(int argc, char **argv) {
  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    int64_t n64;
    char junk;
    if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--num_ops=%d%c", &n, &junk) == 1) {
      FLAGS_num_total_ops = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--gc_threads=%d%c", &n, &junk) == 1) {
      FLAGS_gc_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--log_size_GB=%d%c", &n, &junk) == 1) {
      FLAGS_log_size = (uint64_t)n << 30;
    } else if (sscanf(argv[i], "--init_util=%d%c", &n, &junk) == 1) {
      FLAGS_init_utilization = n;
    } else if (sscanf(argv[i], "--zipf=%d%c", &n, &junk) == 1) {
      FLAGS_YCSB_zipf = n;
    } else {
      std::fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      std::exit(1);
    }
  }

  FLAGS_num_ops = FLAGS_num_total_ops / FLAGS_threads;

  printf("%d serving threads  %d gc threads  init_utilization %d%%\n",
         FLAGS_threads, FLAGS_gc_threads, FLAGS_init_utilization);

  if (FLAGS_value_size < 8) {
    ERROR_EXIT("object size is too small");
  }
  const int object_size = sizeof(KVItem) + sizeof(KeyType) + FLAGS_value_size;

  if (FLAGS_init_utilization > 0) {
    // double init_size = (double)FLAGS_num * object_size * SEGMENT_SIZE /
    //                    LogSegment::SEGMENT_DATA_SIZE;
    double init_size = (double)FLAGS_num * object_size;
    uint64_t total_size = init_size * 100. / FLAGS_init_utilization;
    FLAGS_log_size =
        (total_size + SEGMENT_SIZE - 1) / SEGMENT_SIZE * SEGMENT_SIZE;
    if (FLAGS_log_size < init_size + FLAGS_threads * SEGMENT_SIZE * 2) {
      printf("Warning: not enough space for free segment per thread\n");
      FLAGS_log_size = init_size + FLAGS_threads * SEGMENT_SIZE;
    }
    printf("object size %d Log size %lu num of segments %lu\n", object_size,
           FLAGS_log_size, FLAGS_log_size / SEGMENT_SIZE);
  } else {
    // infinity log space <=> no gc
    uint64_t total_put_ops = FLAGS_num + (uint64_t)FLAGS_num_ops *
                                             (YCSB_Put_Ratio[ycsb_type] + 10) /
                                             100 * FLAGS_threads;
    FLAGS_log_size = total_put_ops * object_size + FLAGS_threads * SEGMENT_SIZE;
    FLAGS_gc_threads = 0;
  }
  DB *db = nullptr;
  std::string db_path = std::string(PMEM_DIR) + "log_kvs";
  std::filesystem::remove_all(db_path);
  std::filesystem::create_directory(db_path);
  db = new DB(db_path, FLAGS_log_size, FLAGS_threads, FLAGS_gc_threads);
  // initialize
  std::vector<KeyType> keys;
  keys.reserve(FLAGS_num);
  for (int i = 0; i < FLAGS_num; i++) {
    keys.emplace_back(getKey(i));
  }
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(1234));

  ThreadArg ta[FLAGS_threads];
  std::thread th[FLAGS_threads];
  SharedState shared(FLAGS_threads);
  shared.keys = keys.data();
  for (int i = 0; i < FLAGS_threads; i++) {
    ta[i].shared = &shared;
    ta[i].db = db;
    ta[i].id = i;
    ta[i].num_ops = FLAGS_num_ops;
    th[i] = std::thread(thread_body, &ta[i]);
  }
#ifdef LOGGING
  ta[0].stats.SetReport();
#endif

  {
    std::unique_lock<std::mutex> lk(shared.mu);
    while (shared.num_initialized < FLAGS_threads) {
      shared.cv.wait(lk);
    }
    // after initialize and load
    keys.clear();
    for (int i = 1; i < FLAGS_threads; i++) {
      ta[0].stats.Merge(ta[i].stats);
    }
    ta[0].stats.Report(Slice("load"));
    // start workload
    shared.start = true;
    shared.cv.notify_all();
    while (shared.num_done < FLAGS_threads) {
      shared.cv.wait(lk);
    }
  }

  for (int i = 0; i < FLAGS_threads; i++) {
    th[i].join();
  }

  for (int i = 1; i < FLAGS_threads; i++) {
    ta[0].stats.Merge(ta[i].stats);
  }
  ta[0].stats.Report(Slice("TEST"));

  printf("Compaction Rate: %.1lf MB/s\n",
         db->GetCompactionThroughput() / (1 << 20));

#ifdef IDX_PERSISTENT
  db->RecoverySegments();
  db->RecoveryInfo();
#else
  db->RecoveryAll();
#endif

  // clean
  delete db;
  return 0;
}
