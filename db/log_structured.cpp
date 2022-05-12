#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>

#include "config.h"
#include "log_structured.h"
#include "log_cleaner.h"

// TODO: meta region
LogStructured::LogStructured(std::string db_path, size_t log_size, DB *db,
                             int num_workers, int num_cleaners)
    : num_workers_(num_workers),
      num_cleaners_(num_cleaners),
      total_log_size_(log_size),
      num_segments_(log_size / SEGMENT_SIZE),
      // max_reserved_segments_(num_cleaners),
      free_list_lock_("free_list"),
      // reserved_list_lock_("reserved_list"),
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
  madvise(pool_start_, total_log_size_, MADV_HUGEPAGE);
#endif
  if (pool_start_ == nullptr || pool_start_ == MAP_FAILED) {
    ERROR_EXIT("mmap failed");
  }
  LOG("Log: pool_start %p total segments: %d  cleaners: %d\n", pool_start_,
      num_segments_, num_cleaners_);
  all_segments_.resize(num_segments_, nullptr);
  int i = 0;
  for (i = 0; i < num_segments_ - num_cleaners_; i++) {
    all_segments_[i] =
        new LogSegment(pool_start_ + i * SEGMENT_SIZE, SEGMENT_SIZE);
    free_segments_.push(all_segments_[i]);
  }
  num_free_segments_ = num_segments_ - num_cleaners_;

  log_cleaners_.resize(num_cleaners_, nullptr);
  for (int j = 0; i < num_segments_; i++, j++) {
    all_segments_[i] =
        new LogSegment(pool_start_ + i * SEGMENT_SIZE, SEGMENT_SIZE);
    log_cleaners_[j] = new LogCleaner(db, j, this, all_segments_[i]);
  }
  for (int j = 0; j < num_cleaners_; j++) {
    log_cleaners_[j]->StartGCThread();
  }

  if (num_cleaners_ == 0) {
    stop_flag_.store(true, std::memory_order_release);
  }
}

LogStructured::~LogStructured() {
  stop_flag_.store(true, std::memory_order_release);
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      log_cleaners_[i]->StopThread();
    }
  }
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      delete log_cleaners_[i];
    }
  }

  for (int i = 0; i < num_segments_; i++) {
    if (all_segments_[i]) {
      delete all_segments_[i];
    }
  }
  all_segments_.clear();

  LOG("num_newSegment %d new_hot %d new_cold %d", num_new_segment_.load(),
      num_new_hot_.load(), num_new_cold_.load());
  munmap(pool_start_, total_log_size_);
  free_list_lock_.report();
}

LogSegment *LogStructured::NewSegment(bool hot) {
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
#ifdef HOT_COLD_SEPARATE
  // not store shortcuts for hot segment
  ret->StartUsing(hot, !hot);
#else
  ret->StartUsing(hot, true);
#endif

  if (hot) {
    COUNTER_ADD_LOGGING(num_new_hot_, 1);
  } else {
    COUNTER_ADD_LOGGING(num_new_cold_, 1);
  }

  UpdateCleanThreshold();
  return ret;
}

void LogStructured::FreezeSegment(LogSegment *old_segment) {
  if (old_segment && num_cleaners_ > 0) {
    old_segment->Close();
    AddClosedSegment(old_segment);
  }
}

void LogStructured::SyncCleanerGarbageBytes(
    std::vector<size_t> &tmp_garbage_bytes) {
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->cleaner_garbage_bytes_ += tmp_garbage_bytes[i];
    tmp_garbage_bytes[i] = 0;
  }
}

LogSegment *LogStructured::GetSegment(int segment_id) {
  assert(segment_id < num_segments_);
  return all_segments_[segment_id];
}

int LogStructured::GetSegmentID(const char *addr) {
  assert(addr >= pool_start_);
  int seg_id = (addr - pool_start_) / SEGMENT_SIZE;
  assert(seg_id < num_segments_);
  return seg_id;
}

int LogStructured::GetSegmentCleanerID(const char *addr) {
  return GetSegmentID(addr) % num_cleaners_;
}

void LogStructured::AddClosedSegment(LogSegment *segment) {
  int cleaner_id = GetSegmentCleanerID(segment->get_segment_start());
  log_cleaners_[cleaner_id]->AddClosedSegment(segment);
}

void LogStructured::StartCleanStatistics() {
  start_clean_statistics_time_ = NowMicros();
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->clean_seg_count_before_ =
        log_cleaners_[i]->clean_seg_count_;
    log_cleaners_[i]->clean_time_ns_before_ = log_cleaners_[i]->clean_time_ns_;
  }
}

double LogStructured::GetCompactionCPUUsage() {
  if (num_cleaners_ == 0) {
    return 0.0;
  }

  uint64_t wall_time = NowMicros() - start_clean_statistics_time_;
  uint64_t total_compaction_time = 0;
  for (int i = 0; i < num_cleaners_; i++) {
    total_compaction_time += log_cleaners_[i]->clean_time_ns_ -
                             log_cleaners_[i]->clean_time_ns_before_;
  }
  double usage = (double)total_compaction_time / 1e3 / wall_time;
  return usage;
}

double LogStructured::GetCompactionThroughput() {
  if (num_cleaners_ == 0) {
    return 0.0;
  }

  double total_tp = 0.;
  for (int i = 0; i < num_cleaners_; i++) {
    int clean_seg_count = log_cleaners_[i]->clean_seg_count_ -
                          log_cleaners_[i]->clean_seg_count_before_;
    uint64_t clean_time_ns = log_cleaners_[i]->clean_time_ns_ -
                             log_cleaners_[i]->clean_time_ns_before_;
    double tp = 1.0 * clean_seg_count * LogSegment::SEGMENT_DATA_SIZE * 1e9 /
                clean_time_ns;
    total_tp += tp;
  }
  return total_tp;
}

void LogStructured::UpdateCleanThreshold() {
  int cnt = alloc_counter_.fetch_add(1);

  if (!FS_flag_.test_and_set()) {
    int num_free = num_free_segments_.load(std::memory_order_relaxed);
    double step = num_segments_ / 200.;  // 0.5%
    double thresh_free = num_segments_ * clean_threshold_ / 100.;

    switch (free_status_) {
      case FS_Sufficient:
        if (num_free < thresh_free) {
          free_status_ = FS_Trigger;
          alloc_counter_ = 0;
        }
        break;
      case FS_Trigger:
        if (num_free < thresh_free - num_workers_ ||
            num_free < num_limit_free_segments_) {
          alloc_counter_ = 0;
          if (num_free > thresh_free - step && clean_threshold_ < 10) {
            // if num_free is much less than thresh, doesn't need to change
            // thresh
            ++clean_threshold_;
          }
          free_status_ = FS_Insufficient;
        } else if (num_free > thresh_free &&
                   thresh_free - step > num_limit_free_segments_ &&
                   clean_threshold_ > 1 && cnt > num_workers_ * 4) {
          alloc_counter_ = 0;
          --clean_threshold_;
          free_status_ = FS_Sufficient;
        }
        break;
      case FS_Insufficient:
        if (num_free >= thresh_free) {
          free_status_ = FS_Sufficient;
        }
        break;
      default:
        break;
    }
    FS_flag_.clear();
  }
}


void LogStructured::RecoverySegments(DB *db) {
  // stop compaction first.
  stop_flag_.store(true, std::memory_order_release);
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      log_cleaners_[i]->StopThread();
    }
  }
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      delete log_cleaners_[i];
      log_cleaners_[i] = nullptr;
    }
  }

  assert(free_segments_.size() == num_free_segments_.load());
  printf("before recovery: %ld free segments\n", free_segments_.size());
  
  // clean segments, simulate a clean restart.
  while (!free_segments_.empty()) {
    free_segments_.pop();
  }
  num_free_segments_ = 0;

  for (int i = 0; i < num_segments_; i++) {
    if (all_segments_[i]) {
      delete all_segments_[i];
    }
  }
  all_segments_.clear();
  all_segments_.resize(num_segments_, nullptr);

  recovery_counter_ = 0;
  
  // start recovery
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i] = new LogCleaner(db, i, this, nullptr);
  }

  uint64_t rec_time = 0;
  TIMER_START(rec_time);
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->StartRecoverySegments();
  }

  // wait for recovery over
  {
    std::unique_lock<std::mutex> lk(rec_mu_);
    while (recovery_counter_ < num_cleaners_) {
      rec_cv_.wait(lk);
    }
  }
  TIMER_STOP(rec_time);
  printf("after recovery: %ld free segments\n", free_segments_.size());
  assert(free_segments_.size() == num_free_segments_.load());
  printf("recovery segments time %lu us\n", rec_time / 1000);
}

void LogStructured::RecoveryInfo(DB *db) {
  if (recovery_counter_ != num_cleaners_) {
    ERROR_EXIT("Should recovery segments first");
  }
  recovery_counter_ = 0;
  uint64_t rec_time = 0;
  TIMER_START(rec_time);
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->StartRecoveryInfo();
  }

  // wait for recovery over
  {
    std::unique_lock<std::mutex> lk(rec_mu_);
    while (recovery_counter_ < num_cleaners_) {
      rec_cv_.wait(lk);
    }
  }
  TIMER_STOP(rec_time);
  printf("recovery info time %lu us\n", rec_time / 1000);
}


void LogStructured::RecoveryAll(DB *db) {
  // stop compaction first.
  stop_flag_.store(true, std::memory_order_release);
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      log_cleaners_[i]->StopThread();
    }
  }
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      delete log_cleaners_[i];
      log_cleaners_[i] = nullptr;
    }
  }

  assert(free_segments_.size() == num_free_segments_.load());
  printf("before recovery: %ld free segments\n", free_segments_.size());
  // clear old segments
  while (!free_segments_.empty()) {
    free_segments_.pop();
  }
  num_free_segments_ = 0;

  for (int i = 0; i < num_segments_; i++) {
    if (all_segments_[i]) {
      delete all_segments_[i];
    }
  }
  all_segments_.clear();
  all_segments_.resize(num_segments_, nullptr);

  // delete old index
#ifndef IDX_PERSISTENT
  db->NewIndexForRecoveryTest();
#endif

  recovery_counter_ = 0;
  
  // start recovery
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i] = new LogCleaner(db, i, this, nullptr);
  }

  uint64_t rec_time = 0;
  TIMER_START(rec_time);
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->StartRecoveryAll();
  }

  // wait for recovery over
  {
    std::unique_lock<std::mutex> lk(rec_mu_);
    while (recovery_counter_ < num_cleaners_) {
      rec_cv_.wait(lk);
    }
  }
  TIMER_STOP(rec_time);
  printf("after recovery: %ld free segments\n", free_segments_.size());
  assert(free_segments_.size() == num_free_segments_.load());
  printf("recovery all time %lu us\n", rec_time / 1000);
}
