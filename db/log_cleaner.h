#pragma once

#include <vector>
#include <queue>

#include "config.h"
#include "db.h"
#include "log_structured.h"
#include "util/util.h"


class LogCleaner {
 public:
  std::atomic<size_t> cleaner_garbage_bytes_{0};

  // clean statistics
#ifdef LOGGING
  uint64_t clean_garbage_bytes_ = 0;
  uint64_t clean_total_bytes_ = 0;
  uint64_t hot_clean_garbage_bytes_ = 0;
  uint64_t hot_clean_total_bytes_ = 0;
  uint64_t cold_clean_garbage_bytes_ = 0;
  uint64_t cold_clean_total_bytes_ = 0;
  uint64_t copy_time_ = 0;
  uint64_t update_index_time_ = 0;
  uint64_t check_liveness_time_ = 0;
  uint64_t pick_time_ = 0;
  int clean_hot_count_ = 0;
  int clean_cold_count_ = 0;
  int flush_pass_ = 0;
  int move_count_ = 0;
  int garbage_move_count_ = 0;
  int fast_path_ = 0;
  int shortcut_cnt_ = 0;
#endif

  int clean_seg_count_ = 0;
  uint64_t clean_time_ns_ = 0;
  int clean_seg_count_before_ = 0;
  uint64_t clean_time_ns_before_ = 0;

  LogCleaner(DB *db, int cleaner_id, LogStructured *log,
             LogSegment *reserved_segment)
      : db_(db),
        cleaner_id_(cleaner_id),
        log_(log),
        reserved_segment_(reserved_segment),
        list_lock_(std::string("gc_list_lock_") + std::to_string(cleaner_id)) {
    tmp_cleaner_garbage_bytes_.resize(db->num_cleaners_, 0);
    if (reserved_segment_) {
      reserved_segment_->StartUsing(false, false);
    }
#ifdef BATCH_COMPACTION
    volatile_segment_ = new VirtualSegment(SEGMENT_SIZE);
    volatile_segment_->set_has_shortcut(false);
#endif
  }

  ~LogCleaner() {
    StopThread();
#ifdef BATCH_COMPACTION
    delete volatile_segment_;
#endif
    LOG("cleaner %d: clean %d (hot %d cold %d) flush_pass %d move %d "
        "move_garbage %d shortcut_cnt %d fast_path %d (%.1lf%%) pick_time %lu "
        "copy_time %lu "
        "check_liveness_time_ %lu update_index_time %lu clean_time_ns %lu "
        "avg_garbage_proportion %.1f%% "
        "hot %.1f%% cold %.1f%%",
        cleaner_id_, clean_seg_count_, clean_hot_count_, clean_cold_count_,
        flush_pass_, move_count_, garbage_move_count_, shortcut_cnt_,
        fast_path_, 100. * fast_path_ / move_count_, pick_time_ / 1000,
        copy_time_ / 1000, check_liveness_time_ / 1000,
        update_index_time_ / 1000, clean_time_ns_ / 1000,
        clean_garbage_bytes_ * 100.f / clean_total_bytes_,
        hot_clean_garbage_bytes_ * 100.f / hot_clean_total_bytes_,
        cold_clean_garbage_bytes_ * 100.f / cold_clean_total_bytes_);

    list_lock_.report();
    if (reserved_segment_) {
#if defined(LOG_BATCHING) && !defined(BATCH_COMPACTION)
      reserved_segment_->FlushRemain();
      BatchIndexUpdate();
#endif
      log_->FreezeSegment(reserved_segment_);
    }
    if (backup_segment_) {
      std::lock_guard<SpinLock> guard(log_->free_list_lock_);
      log_->free_segments_.push(backup_segment_);
      ++log_->num_free_segments_;
    }

    closed_hot_segments_.clear();
    closed_cold_segments_.clear();
    to_compact_hot_segments_.clear();
    to_compact_cold_segments_.clear();
  }

  void StopThread() {
    if (gc_thread_.joinable()) {
      gc_thread_.join();
    }
  }

  void StartGCThread() {
    StopThread();
    gc_thread_ = std::thread(&LogCleaner::CleanerEntry, this);
  }

  void StartRecoverySegments() {
    StopThread();
    gc_thread_ = std::thread(&LogCleaner::RecoverySegments, this);
  }

  void StartRecoveryInfo() {
    StopThread();
    gc_thread_ = std::thread(&LogCleaner::RecoveryInfo, this);
  }

  void StartRecoveryAll() {
    StopThread();
    gc_thread_ = std::thread(&LogCleaner::RecoveryAll, this);
  }

  void AddClosedSegment(LogSegment *segment) {
    LockUsedList();
#ifdef HOT_COLD_SEPARATE
    if (segment->IsHot()) {
      closed_hot_segments_.push_back(segment);
    } else {
      closed_cold_segments_.push_back({segment, 0.});
    }
#else
    closed_hot_segments_.push_back(segment);
#endif
    UnlockUsedList();
  }

 private:
  DB *db_;
  int cleaner_id_;
  LogStructured *log_;
  std::thread gc_thread_;
  VirtualSegment *volatile_segment_ = nullptr;
  LogSegment *reserved_segment_ = nullptr;
  LogSegment *backup_segment_ = nullptr;  // prevent gc dead lock
  std::vector<ValidItem> valid_items_;
  std::vector<size_t> tmp_cleaner_garbage_bytes_;
  double last_update_time_ = 0.;

  std::list<SegmentInfo> closed_cold_segments_;
  std::list<SegmentInfo> to_compact_cold_segments_;
  std::list<LogSegment *> closed_hot_segments_;
  std::list<LogSegment *> to_compact_hot_segments_;
  SpinLock list_lock_;

  bool IsGarbage(KVItem *kv) {
#ifdef WRITE_TOMBSTONE
#ifdef REDUCE_PM_ACCESS
    int log_id = log_->GetSegmentID(reinterpret_cast<char *>(kv));
    return log_->all_segments_[log_id]->IsGarbage(reinterpret_cast<char *>(kv));
#else
    // assert(kv->magic == 0xDEADBEAF);
    return kv->is_garbage;
#endif
#else // not WRITE_TOMBSTONE
    ValueType val = db_->index_->Get(kv->GetKey());
    return TaggedPointer(val).GetAddr() != reinterpret_cast<char *>(kv);
#endif
  }

  void LockUsedList() { list_lock_.lock(); }
  void UnlockUsedList() { list_lock_.unlock(); }

  void CleanerEntry();
  bool NeedCleaning();
  void BatchFlush();
  void BatchIndexUpdate();
  void CopyValidItemToBuffer(LogSegment *segment);
  void BatchCompactSegment(LogSegment *segment);
  void CompactSegment(LogSegment *segment);
  void FreezeReservedAndGetNew();
  void MarkGarbage(ValueType tagged_val);
  void DoMemoryClean();

  void RecoverySegments();
  void RecoveryInfo();
  void RecoveryAll();

  DISALLOW_COPY_AND_ASSIGN(LogCleaner);
};
