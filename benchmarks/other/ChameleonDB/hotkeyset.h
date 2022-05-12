#pragma once

#include <vector>
#include <list>
#include <memory>
#include <thread>
#include <unordered_set>
#include <unordered_map>

#include "slice.h"
#include "config.h"
#include "util/lock.h"

namespace CHAMELEONDB_NAMESPACE {

static constexpr uint64_t HOT_NUM = 128 * 1024;
static constexpr int RECORD_BATCH_CNT = 4096;
static constexpr uint32_t RECORD_BUFFER_SIZE = 16 * 1024;

struct RecordEntry {
  uint64_t key;
  int64_t cnt;

  bool operator>(const RecordEntry &other) const {
    return cnt > other.cnt;
  }
};

struct alignas(CACHE_LINE_SIZE) UpdateKeyRecord {
  int hit_cnt = 0;
  int total_cnt = 0;
  SpinLock lock;
  std::list<std::vector<uint64_t> > records_list;
  std::vector<uint64_t> records;

  UpdateKeyRecord() : lock("") {
    records.reserve(RECORD_BUFFER_SIZE);
  }
};

class ChameleonDB;
class HotKeySet {
 public:
  explicit HotKeySet(ChameleonDB *db);
  ~HotKeySet();

  void Record(const Slice &key, int worker_id, bool hit);
  void BeginUpdateHotKeySet();
  bool Exist(const Slice &key);

 private:
  ChameleonDB *db_;
  std::unordered_set<uint64_t> *current_set_;
  std::unique_ptr<UpdateKeyRecord[]> update_record_;
  std::thread update_hot_set_thread_;
  std::atomic_flag update_schedule_flag_{ATOMIC_FLAG_INIT};
  volatile bool need_record_ = false;
  volatile bool need_count_hit_ = true;
  std::atomic_bool stop_flag_{false};

  void UpdateHotSet();
};

}  // namespace CHAMELEONDB_NAMESPACE
