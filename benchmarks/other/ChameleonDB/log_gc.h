#pragma once

#include <vector>
#include <queue>
#include <utility>

#include "chameleon_db.h"
#include "log.h"
#include "util/util.h"

namespace CHAMELEONDB_NAMESPACE {

class LogGCer {
 public:

  uint64_t clean_time_ns_ = 0;

  LogGCer(ChameleonDB *db, int gc_id, Log *log, LogSegment *reserved_segment)
      : db_(db),
        gc_id_(gc_id),
        log_(log),
        reserved_segment_(reserved_segment),
        backup_segment_(nullptr),
        list_lock_(std::string("gc_list_lock_") + std::to_string(gc_id)) {
    reserved_segment_->StartUsing(false);
  }

  ~LogGCer() {
    gc_thread_.join();
    list_lock_.report();
  }

  void StartGCThread() { gc_thread_ = std::thread(&LogGCer::GCEntry, this); }

  void AddClosedSegment(LogSegment *segment) {
    LockUsedList();
#ifdef HOT_COLD_SEPARATE
    if (segment->IsHot()) {
      closed_hot_segments_.push_back(segment);
    } else {
      closed_cold_segments_.push_back(segment);
    }
#else
    closed_hot_segments_.push_back(segment);
#endif
    UnlockUsedList();
  }

 private:
  ChameleonDB *db_;
  int gc_id_;
  Log *log_;
  std::thread gc_thread_;
  LogSegment *reserved_segment_;
  LogSegment *backup_segment_;  // prevent gc dead lock
  double last_update_time_ = 0;

  std::list<LogSegment *> closed_hot_segments_;
  std::list<LogSegment *> closed_cold_segments_;
  int clean_segment_cnt_ = 0;
  const int gc_hot_per_cold_ = 5;
  SpinLock list_lock_;

  bool IsGarbage(KVItem *kv) {
    ValueType val = db_->IndexGet(kv->GetKey());
    return TaggedPointer(val).GetAddr() != reinterpret_cast<char *>(kv);
    return true;
  }

  void LockUsedList() { list_lock_.lock(); }
  void UnlockUsedList() { list_lock_.unlock(); }

  void GCEntry();
  bool NeedGC();
  void DoMemoryClean();
  void CompactSegment(LogSegment *gc_segment);
  void FreezeReservedAndGetNew();

  DISALLOW_COPY_AND_ASSIGN(LogGCer);
};

} // namespace CHAMELEONDB_NAMESPACE

