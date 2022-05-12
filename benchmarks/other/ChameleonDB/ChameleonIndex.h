#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstring>
#include <functional>

#include "db_common.h"
#include "util/debug_helper.h"
#include "util/hash.h"
#include "util/index_arena.h"
#include "util/lock.h"

namespace CHAMELEONDB_NAMESPACE {

/****** parameter ******/
constexpr size_t kNumShards = 16384;
constexpr size_t kMemTableSize = 8192;
constexpr size_t kNumLevels = 4;
constexpr size_t kBetweenLevelRatio = 4;
constexpr size_t kABISize = 512 * 1024;
constexpr double kLoadFactorLow = 0.65;
constexpr double kLoadFactorHigh = 0.85;
constexpr unsigned kMaxProbeCountInMemTable = 32;


// CAS, FETCH_AND_ADD
#define CAS(_p, _u, _v)                                            \
  __atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQ_REL, \
                              __ATOMIC_ACQUIRE)

#define FAA(_p, _v) __atomic_fetch_add(_p, _v, __ATOMIC_ACQ_REL)
#define AAF(_p, _v) __atomic_add_fetch(_p, _v, __ATOMIC_ACQ_REL)


/**
 * Shard
 * DRAM Part:
 *  MemTable
 *  Auxiliary Bypass Index (ABI)
 * PMem Part:
 *  Hash Tables
 */

typedef KeyType Key_t;
typedef ValueType Value_t;

const Key_t SENTINEL = -2; // 11111...110
const Key_t INVALID = -1;  // 11111...111

struct KVPair {
  Key_t key;
  Value_t val;
};

constexpr size_t kNumMemTableSlot = kMemTableSize / sizeof(KVPair);
constexpr size_t kNumABISlot = kABISize / sizeof(KVPair);

class HashTable {
 public:
  KVPair *records_ = nullptr;
  size_t num_slots_ = 0;
  size_t cur_num_ = 0;
  std::atomic<size_t> max_probe_count_{kMaxProbeCountInMemTable};
  std::function<size_t(const Key_t &)> hash_func;

  void Init(void *table_addr, size_t num_slots,
            std::function<size_t(const Key_t &)> fn) {
    records_ = (KVPair *)table_addr;
    num_slots_ = num_slots;
    hash_func = fn;
    Clear();
  }

  void Clear() {
    cur_num_ = 0;
    max_probe_count_ = kMaxProbeCountInMemTable;
    // init all key to INVALID
    memset(records_, 0xff, sizeof(KVPair) * num_slots_);
  }

  bool Get(const Key_t &key, Value_t *val) {
    if (records_ == nullptr) {
      return false;
    }
    size_t idx = hash_func(key);
    const size_t max_probe_count =
        max_probe_count_.load(std::memory_order_relaxed);
    for (unsigned i = 0; i < max_probe_count; ++i) {
      size_t loc = (idx + i) % num_slots_;
      if (records_[loc].key == key) {
        *val = records_[loc].val;
        return true;
      }
    }
    return false;
  }

  // may exist duplicate insert
  // Insert only in MemTable or ABI, no need to flush
  bool Put(const Key_t &key, const Value_t &val) {
    size_t idx = hash_func(key);
    for (unsigned i = 0; i < kMaxProbeCountInMemTable; ++i) {
      size_t loc = (idx + i) % num_slots_;
      Key_t loc_key = records_[loc].key;
      if (loc_key == key) {
        // same key, update
        records_[loc].val = val;
        return true;
      } else if (loc_key == INVALID) {
        if (CAS(&records_[loc].key, &loc_key, SENTINEL)) {
          records_[loc].val = val;
          compiler_barrier();
          records_[loc].key = key;
          FAA(&cur_num_, 1);
          return true;
        }
      }
    }
    return false;
  }

  void ForcePut(const Key_t &key, const Value_t &val) {
    size_t idx = hash_func(key);
    size_t cur_probe_cnt = 0;
    while (true) {
      size_t loc = (idx + cur_probe_cnt) % num_slots_;
      ++cur_probe_cnt;
      Key_t loc_key = records_[loc].key;
      if (loc_key == key) {
        // same key, update
        records_[loc].val = val;
        break;
      } else if (loc_key == INVALID) {
        if (CAS(&records_[loc].key, &loc_key, SENTINEL)) {
          records_[loc].val = val;
          compiler_barrier();
          records_[loc].key = key;
          ++cur_num_;  // no concurrent, used in compact
          break;
        }
      }
      assert(cur_probe_cnt < num_slots_);
    }
    while (true) {
      size_t old = max_probe_count_.load(std::memory_order_relaxed);
      if (old >= cur_probe_cnt) {
        break;
      }
      if (max_probe_count_.compare_exchange_weak(old, cur_probe_cnt,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
        break;
      }
    }
  }

  double CurLoadFactor() {
    return (double)cur_num_ / num_slots_;
  }
};

struct ShardMeta {
  union {
    struct {
      uint64_t L3ADDR : 48;
      uint64_t L2CNT : 4;
      uint64_t L1CNT : 4;
      uint64_t L0CNT : 4;
      uint64_t padding : 4;
    };
    uint64_t data = 0;
  };
};
static_assert(sizeof(ShardMeta) == 8);

class Shard {
 public:
  Shard() = default;
  ~Shard();
  void Init(int shard_id, ShardMeta *meta, size_t base_seed);
  void UpdateMeta();
  bool Get(const Key_t &key, Value_t *val);
  void Put(const Key_t &key, const Value_t &val);
  void GCPut(const Key_t &key, const Value_t &val);
  // void GCLock() { gc_lock_.WriteLock(); }
  // void GCUnlock() { gc_lock_.WriteUnlock(); }

 private:
  // DRAM
  int shard_id_;
  unsigned int table_count_[kNumLevels - 1];
  HashTable *volatile memtable_ = nullptr;
  HashTable *volatile immutable_memtable_ = nullptr;
  HashTable *volatile ABI_ = nullptr;

  // PMem
  HashTable interlayer_tables_[kNumLevels - 1][kBetweenLevelRatio - 1];
  // last level: records_ and num_slots_ may change
  HashTable *volatile last_level_table_;
  double load_factor_;
  ShardMeta *meta_ = nullptr;  // meta on PM
  std::atomic_flag compact_flag{ATOMIC_FLAG_INIT};
  ReadWriteLock rwlock_;
  // ReadWriteLock gc_lock_;

  void FlushOrCompact(HashTable *old_mt);
  void FlushMemTable(HashTable *old_mt);
  void Compact(HashTable *old_mt);
  void FullCompact(HashTable *old_mt);
  void PartialCompact(HashTable *old_mt, int dest_level, int dest_table_cnt);
};

class ChameleonIndex {
 public:
  std::array<Shard, kNumShards> shards_;

  ChameleonIndex() {
    ShardMeta *meta_addr =
        (ShardMeta *)g_index_allocator->Alloc(sizeof(ShardMeta) * kNumShards);
    for (size_t i = 0; i < kNumShards; ++i) {
      shards_[i].Init(i, &meta_addr[i], i + 1000);
    }
  }
  ~ChameleonIndex() {}

  bool Get(const Key_t &key, Value_t *val) {
    int shard_id = key % kNumShards;
    return shards_[shard_id].Get(key, val);
  }

  void Put(const Key_t &key, const Value_t &val) {
    int shard_id = key % kNumShards;
    shards_[shard_id].Put(key, val);
  }

  bool Delete(const Key_t &key) { ERROR_EXIT("not implemented yet"); }

  bool LockIfValid(const Key_t key, const Value_t val) {
    int shard_id = key % kNumShards;
    // shards_[shard_id].GCLock();
    Value_t v;
    if (shards_[shard_id].Get(key, &v) &&
        TaggedPointer(v).GetAddr() == TaggedPointer(val).GetAddr()) {
      return true;
    }
    // val is not valid
    // shards_[shard_id].GCUnlock();
    return false;
  }

  bool IfValid(const Key_t key, const Value_t val) {
    int shard_id = key % kNumShards;
    Value_t v;
    if (shards_[shard_id].Get(key, &v) &&
        TaggedPointer(v).GetAddr() == TaggedPointer(val).GetAddr()) {
      return true;
    }
    // val is not valid
    return false;
  }

  void GCMoveAndUnlock(const Key_t key, const Value_t new_val) {
    int shard_id = key % kNumShards;
    shards_[shard_id].GCPut(key, new_val);
    // shards_[shard_id].GCUnlock();
  }
};

} // namespace CHAMELEONDB_NAMESPACE
