#include "config.h"
#include "log_cleaner.h"
#if INDEX_TYPE == 3
#include "index_masstree.h"
#endif


void LogCleaner::RecoverySegments() {
  char *log_start = log_->pool_start_;
  int num_cleaners = log_->num_cleaners_;

  int num_free_seg = 0;

  std::queue<LogSegment *> tmp_free_queue;
  for (int i = cleaner_id_; i < log_->num_segments_; i += num_cleaners) {
    LogSegment *seg =
        new LogSegment(log_start + i * SEGMENT_SIZE, SEGMENT_SIZE, false);
    seg->InitBitmap();
#ifdef GC_SHORTCUT
    bool has_shortcut = seg->is_hot_ = !seg->header_->has_shortcut;
    if (has_shortcut) {
      seg->set_has_shortcut(true);
      seg->InitShortcutBuffer();
    }
#endif
    seg->tail_ = seg->data_start_ + seg->header_->offset;

    if (seg->header_->status == StatusAvailable) {
      tmp_free_queue.push(seg);
      ++num_free_seg;
    } else {
      if (seg->header_->status != StatusClosed) {
        ERROR_EXIT("should not happen");
        char *p = seg->get_data_start();
        char *end = seg->get_end();
        while (p < end) {
          KVItem *kv = reinterpret_cast<KVItem *>(p);
          uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
          if (sz == sizeof(KVItem)) {
            break;
          }
          p += sz;
        }
        seg->tail_ = p;
        seg->Close();
      }
      if (seg->is_hot_) {
#ifdef HOT_COLD_SEPARATE
        if (seg->IsHot()) {
          closed_hot_segments_.push_back(seg);
        } else {
          closed_cold_segments_.push_back({seg, 0.});
        }
#else
        closed_hot_segments_.push_back(seg);
#endif
      }
    }
    
    log_->all_segments_[i] = seg;
  }

  {
    std::lock_guard<SpinLock> guard(log_->free_list_lock_);
    int num_free = tmp_free_queue.size();
    while (!tmp_free_queue.empty()) {
      log_->free_segments_.push(tmp_free_queue.front());
      tmp_free_queue.pop();
    }
    log_->num_free_segments_ += num_free;
  }

  // recovery over
  if (++log_->recovery_counter_ == log_->num_cleaners_) {
    log_->rec_cv_.notify_all();
  }
}


void LogCleaner::RecoveryInfo() {
  int num_cleaners = log_->num_cleaners_;

  int recover_seg_cnt = 0;
  int recover_obj_cnt = 0;
  for (int i = cleaner_id_; i < log_->num_segments_; i += num_cleaners) {
    LogSegment *seg = log_->all_segments_[i];

    if (seg->header_->status != StatusAvailable) {
      char *p = seg->get_data_start();
      char *tail = seg->get_tail();
      while (p < tail) {
        KVItem *kv = reinterpret_cast<KVItem *>(p);
        uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
        if (sz == sizeof(KVItem)) {
          break;
        }
        ValueType val = TaggedPointer((char *)kv, sz);
        Slice key = kv->GetKey();
        ValueType real_val = db_->index_->Get(key);
        if (val != real_val) {
          // mark this as garbage
          seg->MarkGarbage((char *)kv, sz);
          // update temp cleaner garbage bytes
          if (db_->num_cleaners_ > 0) {
            tmp_cleaner_garbage_bytes_[cleaner_id_] += sz;
          }
        }
        p += sz;
        ++recover_obj_cnt;
      }
      ++recover_seg_cnt;
    }
  }
  LOG("cleaner %d recover info of %d segments %d objects", cleaner_id_,
      recover_seg_cnt, recover_obj_cnt);

  // recovery over
  if (++log_->recovery_counter_ == log_->num_cleaners_) {
    log_->rec_cv_.notify_all();
  }
}


void LogCleaner::RecoveryAll() {
#if INDEX_TYPE == 3
  reinterpret_cast<MasstreeIndex *>(db_->index_)
      ->MasstreeThreadInit(log_->num_workers_ + cleaner_id_);
#endif
  char *log_start = log_->pool_start_;
  int num_cleaners = log_->num_cleaners_;

  int num_free_seg = 0;

  std::queue<LogSegment *> tmp_free_queue;
  for (int i = cleaner_id_; i < log_->num_segments_; i += num_cleaners) {
    LogSegment *seg =
        new LogSegment(log_start + i * SEGMENT_SIZE, SEGMENT_SIZE, false);
    seg->InitBitmap();
#ifdef GC_SHORTCUT
    bool has_shortcut = seg->header_->has_shortcut;
    seg->is_hot_ = has_shortcut;
    if (has_shortcut) {
      seg->set_has_shortcut(true);
      seg->InitShortcutBuffer();
    }
#endif
    if (seg->header_->status == StatusAvailable) {
      tmp_free_queue.push(seg);
      ++num_free_seg;
    } else {
      if (seg->header_->status != StatusClosed) {
        ERROR_EXIT("should not happend");
      }
      char *p = seg->get_data_start();
      char *end = seg->get_end();
      // temp tail_ for MarkGarbage
      seg->tail_ = end;
      log_->all_segments_[i] = seg;
      
      while (p < end) {
        KVItem *kv = reinterpret_cast<KVItem *>(p);
        uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
        if (sz == sizeof(KVItem)) {
          break;
        }
        ValueType val = TaggedPointer((char *)kv, sz);
        Slice key = kv->GetKey();
        LogEntryHelper le_helper(val);
        db_->index_->Put(key, le_helper);
#ifdef GC_SHORTCUT
        if (has_shortcut) {
          seg->AddShortcut(le_helper.shortcut);
        }
#endif
        if (le_helper.old_val != INVALID_VALUE) {
          // Mark other as garbage
          MarkGarbage(le_helper.old_val);
        }
        seg->cur_cnt_++;
        p += sz;
      }
      seg->tail_ = p;
      seg->Close();

      if (seg->is_hot_) {
#ifdef HOT_COLD_SEPARATE
        if (seg->IsHot()) {
          closed_hot_segments_.push_back(seg);
        } else {
          closed_cold_segments_.push_back({seg, 0.});
        }
#else
        closed_hot_segments_.push_back(seg);
#endif
      }
    }
  }


  {
    std::lock_guard<SpinLock> guard(log_->free_list_lock_);
    int num_free = tmp_free_queue.size();
    while (!tmp_free_queue.empty()) {
      log_->free_segments_.push(tmp_free_queue.front());
      tmp_free_queue.pop();
    }
    log_->num_free_segments_ += num_free;
  }

  // recovery over
  if (++log_->recovery_counter_ == log_->num_cleaners_) {
    log_->rec_cv_.notify_all();
  }
}
