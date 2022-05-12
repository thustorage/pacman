#pragma once

#include <cstdint>
#include <atomic>
#include <libpmem.h>

#include "config.h"
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
class BaseSegment {
 public:

  struct alignas(HEADER_ALIGN_SIZE) Header {
    uint32_t offset; // only valid when status is closed
    uint32_t status;
    uint32_t objects_tail_offset;
    bool has_shortcut;

    void Flush() {
#ifdef LOG_PERSISTENT
      clflushopt_fence(this, sizeof(Header));
#endif
    }
  };

  static constexpr uint32_t HEADERS_SIZE = sizeof(Header);
  static constexpr uint32_t SEGMENT_DATA_SIZE = (SEGMENT_SIZE - HEADERS_SIZE);
  static constexpr uint32_t BYTES_PER_BIT = 32;
  static constexpr uint32_t BITMAP_SIZE =
      (SEGMENT_DATA_SIZE / BYTES_PER_BIT + 7) / 8;

  int cur_cnt_ = 0;

  BaseSegment(char *start_addr, size_t size)
      : segment_start_(start_addr),
        data_start_(start_addr + HEADERS_SIZE),
        end_(start_addr + size) {}

  virtual ~BaseSegment() {}

  bool HasSpaceFor(uint32_t sz) {
    char *tmp_end = tail_ + sz;
#ifdef GC_SHORTCUT
    if (has_shortcut_) {
      tmp_end += sizeof(Shortcut) * (cur_cnt_ + 1);
    }
#endif
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

  uint32_t get_offset() { return tail_ - data_start_; }

  char *get_segment_start() { return segment_start_; }

  char *get_data_start() { return data_start_; }

  char *get_tail() { return tail_; }

  char *get_end() { return end_; }

  bool HasShortcut() { return has_shortcut_; }
  void set_has_shortcut(bool has_shortcut) { has_shortcut_ = has_shortcut; }

 protected:
  union {
    char *const segment_start_; // const
    Header *header_;
  };
  char *const data_start_;
  char *const end_; // const
  char *tail_;
  bool has_shortcut_ = false;

  DISALLOW_COPY_AND_ASSIGN(BaseSegment);
};

class LogSegment : public BaseSegment {
 public:
  LogSegment(char *start_addr, uint64_t size, bool init = true)
      : BaseSegment(start_addr, size), garbage_bytes_(0) {
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
    InitBitmap();
#ifdef LOG_BATCHING
    flush_tail_ = data_start_;
#endif
  }

  void InitBitmap() {
#ifdef REDUCE_PM_ACCESS
    if (volatile_tombstone_) {
      free(volatile_tombstone_);
    }
    volatile_tombstone_ = (uint8_t *)malloc(BITMAP_SIZE);
    memset(volatile_tombstone_, 0, BITMAP_SIZE);
#endif
  }

  void InitShortcutBuffer() {
#ifdef GC_SHORTCUT
    if (shortcut_buffer_) {
      delete shortcut_buffer_;
    }
    if (has_shortcut_) {
      shortcut_buffer_ = new std::vector<Shortcut>();
      shortcut_buffer_->reserve(SEGMENT_DATA_SIZE / 8);
    }
#endif
  }

  virtual ~LogSegment() {
#ifdef REDUCE_PM_ACCESS
    if (volatile_tombstone_) {
      free(volatile_tombstone_);
    }
#endif
#ifdef GC_SHORTCUT
    if (shortcut_buffer_) {
      delete shortcut_buffer_;
      shortcut_buffer_ = nullptr;
    }
#endif
  }

  void StartUsing(bool is_hot, bool has_shortcut = false) {
    header_->status = StatusUsing;
    header_->has_shortcut = has_shortcut;
    header_->Flush();
    is_hot_ = is_hot;
#ifdef GC_SHORTCUT
    has_shortcut_ = has_shortcut;
    InitShortcutBuffer();
#endif
  }

  void Close() {
    if (HasSpaceFor(sizeof(KVItem))) {
      KVItem *end = new (tail_) KVItem();
      end->Flush();
      tail_ += sizeof(KVItem);
    }
#ifdef GC_SHORTCUT
    if (shortcut_buffer_) {
      assert(tail_ + shortcut_buffer_->size() * sizeof(Shortcut) <= end_);
      pmem_memcpy_persist(tail_, shortcut_buffer_->data(),
                          shortcut_buffer_->size() * sizeof(Shortcut));
      assert(shortcut_buffer_->size() == cur_cnt_);
      delete shortcut_buffer_;
      shortcut_buffer_ = nullptr;
    }
#endif
    close_time_ = NowMicros();
    header_->offset = get_offset();
    header_->status = StatusClosed;
    header_->objects_tail_offset = get_offset();
    header_->has_shortcut = has_shortcut_;
    header_->Flush();
  }

  void Clear() {
    tail_ = data_start_;
    garbage_bytes_ = 0;
    is_hot_ = false;
    header_->status = StatusAvailable;
    header_->objects_tail_offset = 0;
    header_->has_shortcut = false;
    header_->Flush();
#ifdef LOG_BATCHING
    not_flushed_cnt_ = 0;
    flush_tail_ = data_start_;
#endif
    has_shortcut_ = false;
    cur_cnt_ = 0;
#ifdef REDUCE_PM_ACCESS
    if (volatile_tombstone_) {
      memset(volatile_tombstone_, 0, BITMAP_SIZE);
    }
#endif
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

#ifdef GC_SHORTCUT
  void AddShortcut(Shortcut sc) {
    if (shortcut_buffer_) {
      shortcut_buffer_->push_back(sc);
    }
  }
#endif

  double GetGarbageProportion() {
    return (double)(garbage_bytes_.load(std::memory_order_relaxed)) /
           get_offset();
  }

  uint64_t get_close_time() { return close_time_; }

  bool IsGarbage(char *p) {
#ifdef REDUCE_PM_ACCESS
    assert(p >= data_start_ && p < tail_);
    int idx = (p - data_start_) / BYTES_PER_BIT;
    int byte = idx / 8;
    int bit = idx % 8;
    return (volatile_tombstone_[byte] >> bit) & 1;
#endif
    assert(0);
    return false;
  }

  void MarkGarbage(char *p, uint32_t sz) {
#ifdef WRITE_TOMBSTONE
    assert(p >= data_start_ && p < tail_);
#ifdef REDUCE_PM_ACCESS
    int idx = (p - data_start_) / BYTES_PER_BIT;
    int byte = idx / 8;
    int bit = idx % 8;
    uint8_t old_val = volatile_tombstone_[byte];
    while (true) {
      assert(((old_val >> bit) & 1) == 0);
      uint8_t new_val = old_val | (1 << bit);
      if (__atomic_compare_exchange_n(&volatile_tombstone_[byte], &old_val,
                                      new_val, true, __ATOMIC_ACQ_REL,
                                      __ATOMIC_ACQUIRE)) {
        break;
      }
    }
#else
    KVItem *kv = reinterpret_cast<KVItem *>(p);
    // assert(kv->is_garbage == false); // may copy a object in gc
    // assert(kv->magic == 0xDEADBEAF);
    kv->is_garbage = true;
    // pmem_clwb((const void *)&kv->is_garbage);
#endif
#endif
    garbage_bytes_ += sz;
  }

  // void set_is_hot(bool hot) { is_hot_ = hot; }
  bool IsHot() { return is_hot_; }

 private:
  uint64_t close_time_;
#ifdef REDUCE_PM_ACCESS
  uint8_t *volatile_tombstone_ = nullptr;
#endif

  std::atomic<int> garbage_bytes_;
  bool is_hot_ = false;

#ifdef GC_SHORTCUT
  std::vector<Shortcut> *shortcut_buffer_ = nullptr;
#endif
#ifdef LOG_BATCHING
  int not_flushed_cnt_ = 0;
  char *flush_tail_ = nullptr;
#endif

  friend class LogCleaner;
  DISALLOW_COPY_AND_ASSIGN(LogSegment);
};

class VirtualSegment : public BaseSegment {
 public:
  VirtualSegment(uint64_t size) : BaseSegment((char *)malloc(size), size) {
    // volatile segment used for cleaner nt-copy
    tail_ = data_start_;
  }

  ~VirtualSegment() { free(segment_start_); }

  void Clear() {
    tail_ = data_start_;
    cur_cnt_ = 0;
  }
};

struct SegmentInfo {
  LogSegment *segment;
  double compaction_score = 0.0;

  bool operator<(const SegmentInfo &other) const {
    return compaction_score > other.compaction_score;
  }
};
