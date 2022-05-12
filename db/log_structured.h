#pragma once

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <tuple>
#include <vector>
#include <memory>

#include "segment.h"
#include "util/lock.h"
#include "util/util.h"


class DB;
class LogCleaner;

enum FreeStatus { FS_Sufficient, FS_Trigger, FS_Insufficient };

class LogStructured {
 public:
  explicit LogStructured(std::string db_path, size_t log_size, DB *db,
                         int num_workers, int num_cleaners);
  ~LogStructured();

  LogSegment *NewSegment(bool hot);
  void FreezeSegment(LogSegment *old_segment);
  void SyncCleanerGarbageBytes(std::vector<size_t> &tmp_garbage_bytes);
  LogSegment *GetSegment(int segment_id);
  int GetSegmentID(const char *addr);
  int GetSegmentCleanerID(const char *addr);

  void StartCleanStatistics();
  double GetCompactionCPUUsage();
  double GetCompactionThroughput();
  void RecoverySegments(DB *db);
  void RecoveryInfo(DB *db);
  void RecoveryAll(DB *db);

  // char *get_pool_start() { return pool_start_; }

 private:
  const int num_workers_;
  const int num_cleaners_;
  char *pool_start_;
  const size_t total_log_size_;
  const int num_segments_;
  // SpinLock reserved_list_lock_;
  std::atomic<bool> stop_flag_{false};
  // const int max_reserved_segments_;
  SpinLock free_list_lock_;

  std::vector<LogSegment *> all_segments_;
  std::vector<LogCleaner *> log_cleaners_;
  std::queue<LogSegment *> free_segments_;
  // std::queue<LogSegment *> reserved_segments_;

  std::atomic<int> num_free_segments_{0};
  std::atomic<int> alloc_counter_{0};
  const int num_limit_free_segments_;
  volatile int clean_threshold_ = 10;

  volatile FreeStatus free_status_ = FS_Sufficient;
  std::atomic_flag FS_flag_{ATOMIC_FLAG_INIT};

  std::atomic<int> recovery_counter_{0};
  std::mutex rec_mu_;
  std::condition_variable rec_cv_;

  // statistics
#ifdef LOGGING
  std::atomic<int> num_new_segment_{0};
  std::atomic<int> num_new_hot_{0};
  std::atomic<int> num_new_cold_{0};
#endif
  uint64_t start_clean_statistics_time_ = 0;

  void AddClosedSegment(LogSegment *segment);
  void LockFreeList() { free_list_lock_.lock(); }
  void UnlockFreeList() { free_list_lock_.unlock(); }
  void UpdateCleanThreshold();

  // LogSegment *NewReservedSegment() {
  //   std::lock_guard<SpinLock> guard(reserved_list_lock_);
  //   LogSegment *segment = nullptr;
  //   if (unlikely(reserved_segments_.empty())) {
  //     ERROR_EXIT("no reserved segment left");
  //   } else {
  //     segment = reserved_segments_.front();
  //     reserved_segments_.pop();
  //   }
  //   assert(segment);
  //   return segment;
  // }

  // bool TryAddReservedSegment(LogSegment *segment) {
  //   std::lock_guard<SpinLock> guard(reserved_list_lock_);
  //   bool ret = reserved_segments_.size() < max_reserved_segments_;
  //   if (ret) {
  //     reserved_segments_.push(segment);
  //   }
  //   return ret;
  // }

  friend class LogCleaner;

  DISALLOW_COPY_AND_ASSIGN(LogStructured);
};
