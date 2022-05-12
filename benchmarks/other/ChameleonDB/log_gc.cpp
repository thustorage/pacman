#include "log_gc.h"

#include <mutex>

namespace CHAMELEONDB_NAMESPACE {

void LogGCer::GCEntry() {
  // bind_core_on_numa(log_->num_workers_ + gc_id_);

  while (!log_->stop_flag_.load(std::memory_order_relaxed)) {
    if (NeedGC()) {
      Timer timer(clean_time_ns_);
      DoMemoryClean();
    } else {
      usleep(1);
    }
  }
}

bool LogGCer::NeedGC() {
  int total_segments = log_->num_segments_ - log_->num_cleaners_;
  int free_segments =
      (uint64_t)(log_->num_free_segments_.load(std::memory_order_relaxed));
  constexpr double threshold = 0.2;
  if (free_segments < total_segments * threshold) {
    return true;
  } else {
    return false;
  }
}

void LogGCer::DoMemoryClean() {
  LogSegment *gc_segment = nullptr;
  LockUsedList();
#ifdef HOT_COLD_SEPARATE
  if (clean_segment_cnt_ % gc_hot_per_cold_ != 0 &&
      !closed_hot_segments_.empty()) {
    gc_segment = closed_hot_segments_.front();
    closed_hot_segments_.pop_front();
  } else if (!closed_cold_segments_.empty()) {
    gc_segment = closed_cold_segments_.front();
    closed_cold_segments_.pop_front();
  }
#else
  if (!closed_hot_segments_.empty()) {
    gc_segment = closed_hot_segments_.front();
    closed_hot_segments_.pop_front();
  }
#endif
  UnlockUsedList();
  if (gc_segment == nullptr) {
    // no closed segment
    return;
  }

  CompactSegment(gc_segment);
}

void LogGCer::CompactSegment(LogSegment *gc_segment) {
  char *p = gc_segment->get_data_start();
  char *tail = gc_segment->get_tail();
  while (p < tail) {
    KVItem *kv = reinterpret_cast<KVItem *>(p);
    uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
    if (sz == sizeof(KVItem)) {
      break;
    }
    if (!reserved_segment_->HasSpaceFor(sz)) {
      FreezeReservedAndGetNew();
    }
    Slice key_slice = kv->GetKey();
    KeyType key = *(KeyType *)(key_slice.data());
    ValueType old_val = TaggedPointer(p, sz);
    // if (db_->LockIfValid(key, old_val)) {
    //   ValueType new_val =
    //       reserved_segment_->Append(key_slice, kv->GetValue(), kv->epoch);
    //   db_->GCMoveAndUnlock(key, new_val);
    // }
    if (db_->IfValid(key, old_val)) {
      ValueType new_val =
          reserved_segment_->Append(key_slice, kv->GetValue(), kv->epoch);
      db_->IndexPut(key_slice, new_val);
    }
    p += sz;
  }

  db_->thread_status_.rcu_barrier();
  ++clean_segment_cnt_;
  gc_segment->Clear();
  if (backup_segment_ == nullptr) {
    backup_segment_ = gc_segment;
  } else {
    std::lock_guard<SpinLock> guard(log_->free_list_lock_);
    log_->free_segments_.push(gc_segment);
    ++log_->num_free_segments_;
  }
}

void LogGCer::FreezeReservedAndGetNew() {
  assert(backup_segment_);
  log_->FreezeSegment(reserved_segment_);
  reserved_segment_ = backup_segment_;
  reserved_segment_->StartUsing(false);
  backup_segment_ = nullptr;
}

} // namespace CHAMELEONDB_NAMESPACE
