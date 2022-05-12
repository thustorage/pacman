#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <mutex>


#include "chameleon_db.h"
#include "log.h"
#include "log_gc.h"

namespace CHAMELEONDB_NAMESPACE {

Log::Log(std::string db_path, size_t log_size, ChameleonDB *db, int num_workers,
         int num_cleaners)
    : num_workers_(num_workers),
      num_cleaners_(num_cleaners),
      total_log_size_(log_size),
      num_segments_(log_size / SEGMENT_SIZE),
      free_list_lock_("free_list"),
      num_limit_free_segments_(num_workers * (10 - num_cleaners)) {
#ifdef LOG_PERSISTENT
  std::string log_pool_path = db_path + "/log_pool";
  int log_pool_fd = open(log_pool_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (log_pool_fd < 0) {
    ERROR_EXIT("open file failed");
  }
  if (fallocate(log_pool_fd, 0, 0, total_log_size_) != 0) {
    ERROR_EXIT("fallocate file failed");
  }
  pool_start_ = (char *)mmap(NULL, total_log_size_, PROT_READ | PROT_WRITE,
                             MAP_SHARED, log_pool_fd, 0);
  close(log_pool_fd);
#else
  pool_start_ = (char *)mmap(NULL, total_log_size_, PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_ANONYMOUS, -1, 0);
#endif
  if (pool_start_ == nullptr || pool_start_ == MAP_FAILED) {
    ERROR_EXIT("mmap failed");
  }
  LOG("Log: pool_start %p total segments: %d  cleaners: %d\n", pool_start_,
      num_segments_, num_cleaners_);
  all_segments_.resize(num_segments_, nullptr);
  int i = 0;
  for (i = 0; i < num_segments_ - num_cleaners; i++) {
    all_segments_[i] =
        new LogSegment(pool_start_ + i * SEGMENT_SIZE, SEGMENT_SIZE);
    free_segments_.push(all_segments_[i]);
  }
  num_free_segments_ = num_segments_ - num_cleaners_;

  log_cleaners_.resize(num_cleaners_, nullptr);
  for (int j = 0; i < num_segments_; i++, j++) {
    all_segments_[i] =
        new LogSegment(pool_start_ + i * SEGMENT_SIZE, SEGMENT_SIZE);
    log_cleaners_[j] = new LogGCer(db, j, this, all_segments_[i]);
  }
  for (int j = 0; j < num_cleaners_; j++) {
    log_cleaners_[j]->StartGCThread();
  }

  if (num_cleaners_ == 0) {
    stop_flag_.store(true, std::memory_order_release);
  }
}

Log::~Log() {
  stop_flag_.store(true, std::memory_order_release);
  for (int i = 0; i < num_cleaners_; i++) {
    delete log_cleaners_[i];
  }

  LOG("num_newSegment %d new_hot %d new_cold %d", num_new_segment_.load(),
      num_new_hot_.load(), num_new_cold_.load());
  munmap(pool_start_, total_log_size_);
  free_list_lock_.report();
}

LogSegment *Log::NewSegment(bool hot) {
  LogSegment *ret = nullptr;
  // uint64_t waiting_time = 0;
  // TIMER_START(waiting_time);
  while (true) {
    if (num_free_segments_ > 0) {
      std::lock_guard<SpinLock> guard(free_list_lock_);
      if (!free_segments_.empty()) {
        ret = free_segments_.front();
        free_segments_.pop();
        --num_free_segments_;
      }
    } else {
      if (num_cleaners_ == 0) {
        ERROR_EXIT("No free segments and no cleaners");
      }
      usleep(1);
    }
    if (ret) {
      break;
    }
  }
  // TIMER_STOP(waiting_time);

  COUNTER_ADD_LOGGING(num_new_segment_, 1);
  ret->StartUsing(hot);
  if (hot) {
    COUNTER_ADD_LOGGING(num_new_hot_, 1);
  } else {
    COUNTER_ADD_LOGGING(num_new_cold_, 1);
  }

  return ret;
}

void Log::FreezeSegment(LogSegment *old_segment) {
  if (old_segment && num_cleaners_ > 0) {
    old_segment->Close();
    AddClosedSegment(old_segment);
  }
}

LogSegment *Log::GetSegment(int segment_id) {
  assert(segment_id < num_segments_);
  return all_segments_[segment_id];
}

int Log::GetSegmentID(const char *addr) {
  assert(addr >= pool_start_);
  int seg_id = (addr - pool_start_) / SEGMENT_SIZE;
  assert(seg_id < num_segments_);
  return seg_id;
}

int Log::GetSegmentCleanerID(const char *addr) {
  return GetSegmentID(addr) % num_cleaners_;
}

void Log::AddClosedSegment(LogSegment *segment) {
  int cleaner_id = GetSegmentCleanerID(segment->get_segment_start());
  log_cleaners_[cleaner_id]->AddClosedSegment(segment);
}


}  // namespace CHAMELEONDB_NAMESPACE
