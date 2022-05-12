#pragma once

#include "db_common.h"
#include "util/util.h"

// static constexpr int NUM_HEADERS = 1;
static constexpr int HEADER_ALIGN_SIZE = 256;
// weird slow when 4 * 64
// rotating counter with multi logs reduce performance

/**
 * segment header
 * tail pointer (offset): 4 bytes
 * status: free, in-used, close
 */
enum SegmentStatus { StatusAvailable, StatusUsing, StatusClosed };
class LogSegment {
 public:
  struct alignas(HEADER_ALIGN_SIZE) Header {
    uint32_t offset;  // only valid when status is closed
    uint32_t status;
    uint32_t objects_tail_offset;

    void Flush() {
#ifdef LOG_PERSISTENT
      clflushopt_fence(this, sizeof(Header));
#endif
    }
  };

  static constexpr uint32_t HEADERS_SIZE = sizeof(Header);
  static constexpr uint32_t SEGMENT_DATA_SIZE = (SEGMENT_SIZE - HEADERS_SIZE);

  int cur_cnt_ = 0;

  LogSegment(char *start_addr, uint64_t size, bool init = true)
      : segment_start_(start_addr),
        data_start_(start_addr + HEADERS_SIZE),
        end_(start_addr + size) {
    assert(((uint64_t)header_ & (HEADER_ALIGN_SIZE - 1)) == 0);
    if (init) {
      Init();
    }
  }

  void Init() {
    tail_ = data_start_;
    // header_->offset = 0;
    header_->objects_tail_offset = 0;
    header_->status = StatusAvailable;
    header_->Flush();
#ifdef LOG_BATCHING
    flush_tail_ = data_start_;
#endif
  }

  bool HasSpaceFor(uint32_t sz) {
    char *tmp_end = tail_ + sz;
    return tmp_end <= end_;
  }

  char *AllocOne(size_t size) {
    char *ret = tail_;
    if (ret + size <= end_) {
      tail_ += size;
      ++cur_cnt_;
      return ret;
    } else {
      return nullptr;
    }
  }

  char *AllocSpace(size_t size) {
    char *ret = tail_;
    if (ret + size <= end_) {
      tail_ += size;
      return ret;
    } else {
      return nullptr;
    }
  }

  void StartUsing(bool is_hot, bool has_shortcut = false) {
    header_->status = StatusUsing;
    header_->Flush();
    is_hot_ = is_hot;
  }

  void Close() {
    if (HasSpaceFor(sizeof(KVItem))) {
      KVItem *end = new (tail_) KVItem();
      end->Flush();
      tail_ += sizeof(KVItem);
    }

    close_time_ = NowMicros();
    header_->offset = get_offset();
    header_->status = StatusClosed;
    header_->objects_tail_offset = get_offset();
    header_->Flush();
  }

  void Clear() {
    tail_ = data_start_;
    is_hot_ = false;
    header_->status = StatusAvailable;
    header_->objects_tail_offset = 0;
    header_->Flush();
#ifdef LOG_BATCHING
    not_flushed_cnt_ = 0;
    flush_tail_ = data_start_;
#endif
    cur_cnt_ = 0;
  }

  // append kv to log
  ValueType Append(const Slice &key, const Slice &value, uint32_t epoch) {
    uint32_t sz = sizeof(KVItem) + key.size() + value.size();
    if (!HasSpaceFor(sz)) {
      return INVALID_VALUE;
    }
    KVItem *kv = new (tail_) KVItem(key, value, epoch);
    kv->Flush();
    tail_ += sz;
    ++cur_cnt_;
    return TaggedPointer((char *)kv, sz);
  }

#ifdef LOG_BATCHING
  int FlushRemain() {
    clwb_fence(flush_tail_, tail_ - flush_tail_);
    flush_tail_ = tail_;
    int persist_cnt = not_flushed_cnt_;
    not_flushed_cnt_ = 0;
    return persist_cnt;
  }

  ValueType AppendBatchFlush(const Slice &key, const Slice &value,
                             uint32_t epoch, int *persist_cnt) {
    uint32_t sz = sizeof(KVItem) + key.size() + value.size();
    if (!HasSpaceFor(sz)) {
      return INVALID_VALUE;
    }
    KVItem *kv = new (tail_) KVItem(key, value, epoch);
    ++not_flushed_cnt_;
    tail_ += sz;
    ++cur_cnt_;
    char *align_addr = (char *)((uint64_t)tail_ & ~(LOG_BATCHING_SIZE - 1));
    if (align_addr - flush_tail_ >= LOG_BATCHING_SIZE) {
      clwb_fence(flush_tail_, align_addr - flush_tail_);
      flush_tail_ = align_addr;
      if (tail_ == align_addr) {
        *persist_cnt = not_flushed_cnt_;
        not_flushed_cnt_ = 0;
      } else {
        *persist_cnt = not_flushed_cnt_ - 1;
        not_flushed_cnt_ = 1;
      }
    } else {
      *persist_cnt = 0;
    }
    return TaggedPointer((char *)kv, sz);
  }
#endif

  uint64_t get_close_time() { return close_time_; }

  bool IsHot() { return is_hot_; }

  uint32_t get_offset() { return tail_ - data_start_; }
  char *get_segment_start() { return segment_start_; }
  char *get_data_start() { return data_start_; }
  char *get_tail() { return tail_; }
  char *get_end() { return end_; }

 private:
  union {
    char *const segment_start_;  // const
    Header *header_;
  };
  char *const data_start_;
  char *const end_;  // const
  char *tail_;
  uint64_t close_time_;
  bool is_hot_ = false;

#ifdef LOG_BATCHING
  int not_flushed_cnt_ = 0;
  char *flush_tail_ = nullptr;
#endif

  friend class LogGCer;
  DISALLOW_COPY_AND_ASSIGN(LogSegment);
};
