#pragma once

#include "segment.h"
#include "util/util.h"


namespace CHAMELEONDB_NAMESPACE {

class ChameleonDB;
class LogGCer;

enum FreeStatus { FS_Sufficient, FS_Trigger, FS_Insufficient };

class Log {
 public:
  explicit Log(std::string db_path, size_t log_size, ChameleonDB *db,
               int num_workers, int num_cleaners);
  ~Log();

  LogSegment *NewSegment(bool hot);
  void FreezeSegment(LogSegment *old_segment);
  LogSegment *GetSegment(int segment_id);
  int GetSegmentID(const char *addr);
  int GetSegmentCleanerID(const char *addr);

 private:
  const int num_workers_;
  const int num_cleaners_;
  char *pool_start_;
  const size_t total_log_size_;
  const int num_segments_;
  std::atomic<bool> stop_flag_{false};
  SpinLock free_list_lock_;

  std::vector<LogSegment *> all_segments_;
  std::queue<LogSegment *> free_segments_;
  std::vector<LogGCer *> log_cleaners_;

  std::atomic<int> num_free_segments_{0};
  std::atomic<int> alloc_counter_{0};
  const int num_limit_free_segments_;

  volatile FreeStatus free_status_ = FS_Sufficient;
  std::atomic_flag FS_flag_{ATOMIC_FLAG_INIT};

  // statistics
#ifdef LOGGING
  std::atomic<int> num_new_segment_{0};
  std::atomic<int> num_new_hot_{0};
  std::atomic<int> num_new_cold_{0};
#endif

  void AddClosedSegment(LogSegment *segment);
  void LockFreeList() { free_list_lock_.lock(); }
  void UnlockFreeList() { free_list_lock_.unlock(); }

  friend class LogGCer;
  DISALLOW_COPY_AND_ASSIGN(Log);
};

}  // namespace CHAMELEONDB_NAMESPACE
