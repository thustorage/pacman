/**
 * This code was taken and modified from https://github.com/DICL/CCEH, the original authors of CCEH.
 *
 * Orignial License:
 * Copyright (c) 2018, Sungkyunkwan University. All rights reserved.
 * The license is a free non-exclusive, non-transferable license to reproduce,
 * use, modify and display the source code version of the Software, with or
 * without modifications solely for non-commercial research, educational or
 * evaluation purposes. The license does not entitle Licensee to technical
 * support, telephone assistance, enhancements or updates to the Software. All
 * rights, title to and ownership interest in the Software, including all
 * intellectual property rights therein shall remain in Sungkyunkwan University.
 */

#pragma once

/**
 * Define this to use CCEH in PMem instead of DRAM.
 * Change the file location (CCEH_PMEM_POOL_FILE) above the PMemAllocator definition to a location of your choice.
 */
#ifdef IDX_PERSISTENT
#define CCEH_PERSISTENT
#endif

#include <cstring>
#include <cmath>
#include <vector>
#include <stdint.h>
#include <iostream>
#include <cmath>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include <atomic>
#include <stdlib.h>

#include "hash.hpp"
#include "config.h"

#ifdef CCEH_PERSISTENT
#include <libpmemobj.h>
// #include <libpmemobj++/allocator.hpp>
// #include <libpmempool.h>
#endif

namespace viper {

#ifdef CCEH_PERSISTENT
// static constexpr char CCEH_PMEM_POOL_FILE[] = "/mnt/pmem0/viper/cceh-allocator.file";
static constexpr uint64_t POOL_SIZE = 10ul << 30;
class PMemAllocator {
  public:
    static PMemAllocator& get() {
        static std::once_flag flag;
        std::call_once(flag, []{ instance(); });
        return instance();
    }

    void allocate(PMEMoid* pmem_ptr, size_t size) {
        auto ctor = [](PMEMobjpool* pool, void* ptr, void* arg) { return 0; };
        // int ret = pmemobj_alloc(pmem_pool_.handle(), pmem_ptr, size, 0, ctor, nullptr);
        int ret = pmemobj_alloc(pop_, pmem_ptr, size, 0, ctor, nullptr);
        if (ret != 0) {
            throw std::runtime_error{std::string("Could not allocate! ") + std::strerror(errno)};
        }
    }

    static PMemAllocator& instance() {
        static PMemAllocator instance{};
        return instance;
    }

    PMemAllocator() {
      CCEH_PMEM_POOL_FILE = std::string(PMEM_DIR) + "viper/cceh-allocator.file";
      pool_is_open_ = false;
      initialize();
    }

    void initialize() {
        destroy();

        int sds_write_value = 0;
        // pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
        // pmem_pool_ = pmem::obj::pool_base::create(CCEH_PMEM_POOL_FILE, "", 10ul * (1024l * 1024 * 1024), S_IRWXU);
        pop_ = pmemobj_create(CCEH_PMEM_POOL_FILE.c_str(), "CCEH", POOL_SIZE, S_IRWXU);
        if (pop_ == nullptr) {
            throw std::runtime_error("Could not open allocator pool file.");
        }
        printf("use persistent cceh\n");
        pool_is_open_ = true;
    }

    void destroy() {
        if (pool_is_open_) {
            // pmem_pool_.close();
            pmemobj_close(pop_);
            pool_is_open_ = false;
        }
        std::filesystem::remove(CCEH_PMEM_POOL_FILE);
        // pmempool_rm(CCEH_PMEM_POOL_FILE, PMEMPOOL_RM_FORCE);
    }

    ~PMemAllocator() {
        destroy();
    }

    // pmem::obj::pool_base pmem_pool_;
    PMEMobjpool *pop_;
    bool pool_is_open_;
    std::string CCEH_PMEM_POOL_FILE;
};
#endif


#define internal_cas(entry, expected, updated) \
    __atomic_compare_exchange_n(entry, expected, updated, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)

#define ATOMIC_LOAD(addr) \
    __atomic_load_n(addr, __ATOMIC_ACQUIRE)

#define ATOMIC_STORE(addr, value) \
    __atomic_store_n(addr, value, __ATOMIC_RELEASE)

#define requires_fingerprint(KeyType) \
    std::is_same_v<KeyType, std::string> || sizeof(KeyType) > 8

#define IS_BIT_SET(variable, mask) ((variable & mask) != 0)


template <typename KeyType>
inline bool CAS(KeyType* key, KeyType* expected, KeyType updated) {
    if constexpr (sizeof(KeyType) == 1) return internal_cas((int8_t*) key, (int8_t*) expected, (int8_t) updated);
    else if constexpr (sizeof(KeyType) == 2) return internal_cas((int16_t*) key, (int16_t*) expected, (int16_t) updated);
    else if constexpr (sizeof(KeyType) == 4) return internal_cas((int32_t*) key, (int32_t*) expected, (int32_t) updated);
    else if constexpr (sizeof(KeyType) == 8) return internal_cas((int64_t*) key, (int64_t*) expected, (int64_t) updated);
    else if constexpr (sizeof(KeyType) == 16) return internal_cas((__int128*) key, (__int128*) expected, (__int128) updated);
    else throw std::runtime_error("CAS not supported for > 16 bytes!");
}


using offset_size_t = uint64_t;
using block_size_t = uint64_t;
using page_size_t = uint8_t;
using data_offset_size_t = uint16_t;

struct KeyValueOffset {
    static constexpr offset_size_t INVALID = 0xFFFFFFFFFFFFFFFF;

    union {
        offset_size_t offset;
        struct {
            offset_size_t block_number : 29;
            offset_size_t page_number : 3;
            offset_size_t data_offset : 16;
            offset_size_t object_size : 16;
        };
        // struct {
        //     block_size_t block_number : 45;
        //     page_size_t page_number : 3;
        //     data_offset_size_t data_offset : 16;
        // };
    };

    KeyValueOffset() : offset{INVALID} {}

    static KeyValueOffset NONE() { return KeyValueOffset{INVALID}; }

    explicit KeyValueOffset(const offset_size_t offset) : offset(offset) {}

    KeyValueOffset(const block_size_t block_number,
                   const page_size_t page_number, const data_offset_size_t slot,
                   const data_offset_size_t size = 0)
        : block_number{block_number},
          page_number{page_number},
          data_offset{slot},
          object_size(size) {}

    static KeyValueOffset Tombstone() {
        return KeyValueOffset{};
    }

    inline std::tuple<block_size_t, page_size_t, data_offset_size_t> get_offsets() const {
        return {block_number, page_number, data_offset};
    }

    inline bool is_tombstone() const {
        return offset == INVALID;
    }

    inline bool operator==(const KeyValueOffset& rhs) const { return offset == rhs.offset; }
    inline bool operator!=(const KeyValueOffset& rhs) const { return offset != rhs.offset; }
};
static_assert(sizeof(KeyValueOffset) == sizeof(uint64_t));

using IndexK = size_t;
using IndexV = KeyValueOffset;
constexpr IndexK SENTINEL = -2; // 11111...110
constexpr IndexK INVALID = -1; // 11111...111


// shortcut
class __attribute__((__packed__)) Shortcut {
 public:
  Shortcut() : addr_(0), pos_(0) {}
  Shortcut(char *node_addr, int pos) {
    assert(((uint64_t)node_addr & 0xf) == 0);
    addr_ = (uint64_t)node_addr >> 4;
    pos_ = pos;
  }

  char *GetNodeAddr() const {
    return (char *)((uint64_t)addr_ << 4);
  }

  int GetPos() const { return pos_; }

  bool None() { return addr_ == 0; }

 private:
  uint64_t addr_ : 43;
  uint8_t pos_ : 5;
};
static_assert(sizeof(Shortcut) == 6);

namespace cceh {

#define CACHE_LINE_SIZE 64

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits)-1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 4;

constexpr uint64_t SPLIT_REQUEST_BIT = 1ul << 63;
constexpr uint64_t EXCLUSIVE_LOCK = -1;

struct Pair {
    IndexK key;
    IndexV value;

    Pair(void) : key{INVALID}, value{IndexV::Tombstone()} {}

    Pair(IndexK _key, IndexV _value) : key{_key}, value{_value} {}

    Pair& operator=(const Pair& other) {
        key = other.key;
        value = other.value;
        return *this;
    }
};

// static inline void
// pmem_clflushopt(const void *addr)
// {
// 	asm volatile(".byte 0x66; clflush %0" : "+m" \
// 		(*(volatile char *)(addr)));
// }

static inline void
clflush_fence(const void *addr, size_t len)
{
	uintptr_t uptr;
	for (uptr = (uintptr_t)addr & ~(CACHE_LINE_SIZE - 1);
		uptr < (uintptr_t)addr + len; uptr += CACHE_LINE_SIZE)
		_mm_clflush((char *)uptr);
	_mm_sfence();
}

inline void persist(void* data, size_t len) {
#ifdef CCEH_PERSISTENT
//   pmem_persist(data, len);
    clflush_fence(data, len);
#endif
}

template <typename KeyType>
struct Segment {
    static const size_t kNumSlot = kSegmentSize / sizeof(Pair);

    Segment(void)
        : local_depth{0}
    { }

    Segment(size_t depth)
        :local_depth{depth}
    { }

    void* operator new(size_t size) {
#ifdef CCEH_PERSISTENT
        PMEMoid ret;
        PMemAllocator::get().allocate(&ret, size);
        return pmemobj_direct(ret);
#else
        void* ret;
        if (posix_memalign(&ret, 64, size) != 0) throw std::runtime_error("bad memalign");
        return ret;
#endif
    }

    void *operator new[](size_t size) {
#ifdef CCEH_PERSISTENT
        PMEMoid ret;
        PMemAllocator::get().allocate(&ret, size);
        return pmemobj_direct(ret);
#else
        void* ret;
        if (posix_memalign(&ret, 64, size) != 0) throw std::runtime_error("bad memalign");
        return ret;
#endif
    }

    template <typename KeyCheckFn>
    int Insert(const KeyType&, IndexV, size_t, size_t, IndexV* old_entry, KeyCheckFn, Shortcut *sc = nullptr);

    void Insert4split(IndexK, IndexV, size_t);
    Segment** Split(void);

    Pair _[kNumSlot];
    size_t local_depth;
    std::atomic<uint64_t> sema = 0;
    size_t pattern = 0;
    static constexpr bool using_fp_ = requires_fingerprint(KeyType);
};

template <typename KeyType>
struct Directory {
    static const size_t kDefaultDepth = 10;
    Segment<KeyType>** _;
    size_t capacity;
    size_t depth;
    bool lock;
#ifdef CCEH_PERSISTENT
    PMEMoid pmem_seg_loc_;
#endif

    Directory(void) {
        depth = kDefaultDepth;
        capacity = pow(2, depth);
        // _ = new Segment<KeyType>*[capacity];
#ifdef CCEH_PERSISTENT
        PMemAllocator::get().allocate(&pmem_seg_loc_, sizeof(Segment<KeyType>*) * capacity);
        _ = (Segment<KeyType>**) pmemobj_direct(pmem_seg_loc_);
#else
        _ = new Segment<KeyType>*[capacity];
#endif
        lock = false;
    }

    Directory(size_t _depth) {
        depth = _depth;
        capacity = pow(2, depth);
#ifdef CCEH_PERSISTENT
        PMemAllocator::get().allocate(&pmem_seg_loc_, sizeof(Segment<KeyType>*) * capacity);
        _ = (Segment<KeyType>**) pmemobj_direct(pmem_seg_loc_);
#else
        _ = new Segment<KeyType>*[capacity];
#endif
        lock = false;
    }

    ~Directory(void) {
#ifdef CCEH_PERSISTENT
        pmemobj_free(&pmem_seg_loc_);
#else
        delete [] _;
#endif
    }

    bool Acquire(void) {
        bool unlocked = false;
        return CAS(&lock, &unlocked, true);
    }

    bool Release(void) {
        bool locked = true;
        return CAS(&lock, &locked, false);
    }

    void* operator new(size_t size) {
#ifdef CCEH_PERSISTENT
        PMEMoid ret;
        PMemAllocator::get().allocate(&ret, size);
        return pmemobj_direct(ret);
#else
        void* ret;
        if (posix_memalign(&ret, 64, size) != 0) throw std::runtime_error("bad memalign");
        return ret;
#endif
    }

#ifdef CCEH_PERSISTENT
    void operator delete(void* addr) {}
#endif
};

template <typename KeyType>
class CCEH {
  public:
    static constexpr auto dummy_key_check = [](const KeyType&, IndexV) {
        throw std::runtime_error("Dummy key check should never be used!");
        return true;
    };

    CCEH(size_t);
    ~CCEH();

    template <typename KeyCheckFn>
    IndexV Insert(const KeyType&, IndexV, KeyCheckFn, Shortcut *sc = nullptr);
    template <typename KeyCheckFn>
    bool TryGCUpdate(const KeyType&, IndexV, KeyCheckFn, IndexV*, Shortcut sc);

    template <typename KeyCheckFn>
    IndexV Get(const KeyType&, KeyCheckFn);

    template <typename KeyCheckFn>
    bool Delete(const KeyType&, KeyCheckFn);

    IndexV Insert(const KeyType&, IndexV, Shortcut *sc = nullptr);
    bool Delete(const KeyType&);
    IndexV Get(const KeyType&);
    void Remove(IndexV* offset);
    size_t Capacity(void);

  private:
    Directory<KeyType>* dir;
    static constexpr bool using_fp_ = requires_fingerprint(KeyType);
};

extern size_t perfCounter;

template <typename KeyType>
template <typename KeyCheckFn>
int Segment<KeyType>::Insert(const KeyType& key, IndexV value, size_t loc, size_t key_hash,
                             IndexV* old_entry, KeyCheckFn key_check_fn, Shortcut *sc) {
  uint64_t lock = sema.load();
  if (lock == EXCLUSIVE_LOCK) return 2;
  if (IS_BIT_SET(lock, SPLIT_REQUEST_BIT)) return 1;

  const size_t pattern_shift = 8 * sizeof(key_hash) - local_depth;
  if ((key_hash >> pattern_shift) != pattern) return 2;

  int ret = 1;
  while (!sema.compare_exchange_weak(lock, lock+1)) {
      if (lock == EXCLUSIVE_LOCK) return 2;
      if (IS_BIT_SET(lock, SPLIT_REQUEST_BIT)) return 1;
  }

  IndexK LOCK = INVALID;
  IndexK key_checker;
  if constexpr (using_fp_) {
      key_checker = key_hash;
  } else {
      key_checker = *reinterpret_cast<const IndexK*>(&key);
  }

  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    if (ATOMIC_LOAD(&_[slot].key) == key_checker) {
      if constexpr (using_fp_) {
        // FPs matched but not necessarily the actual key.
        const bool keys_match = key_check_fn(key, _[slot].value);
        if (!keys_match) continue;
      }

      IndexV old_value = _[slot].value;
      while (!CAS(&_[slot].value.offset, &old_value.offset, value.offset)) {}
      if (value.is_tombstone()) {
        IndexK expected = key_checker;
        CAS(&_[slot].key, &expected, INVALID);
      }
      old_entry->offset = old_value.offset;
      if (sc) {
          *sc = Shortcut(reinterpret_cast<char *>(this), i);
      }
      persist(&_[slot].key, sizeof(Pair));
      ret = 0;
      sema.fetch_sub(1);
      return ret;
    }
  }

  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    auto _key = _[slot].key;

    bool invalidate = _key != INVALID;
    if constexpr (using_fp_) {
        invalidate &= (_key >> pattern_shift) != pattern;
    } else {
        invalidate &= (h(&_key, sizeof(IndexK)) >> pattern_shift) != pattern;
    }

    if (invalidate && CAS(&_[slot].key, &_key, INVALID)) {
        _[slot].value = IndexV::Tombstone();
    }

    if (CAS(&_[slot].key, &LOCK, SENTINEL)) {
        // old_entry->offset = _[slot].value.offset;
        old_entry->offset = IndexV::Tombstone().offset;
        _[slot].value = value;
        if (value.is_tombstone()) {
            // Inserted tombstone
            _[slot].key = INVALID;
        }
        else {
            _[slot].key = key_checker;
        }
        if (sc) {
            *sc = Shortcut(reinterpret_cast<char *>(this), i);
        }
        persist(&_[slot], sizeof(Pair));
        ret = 0;
        break;
    } else if (ATOMIC_LOAD(&_[slot].key) == key_checker) {
        if constexpr (using_fp_) {
            // FPs matched but not necessarily the actual key.
            const bool keys_match = key_check_fn(key, _[slot].value);
            if (!keys_match) continue;
        }
        assert(false);
        IndexV old_value = _[slot].value;
        while (!CAS(&_[slot].value.offset, &old_value.offset, value.offset)) {}
        if (value.is_tombstone()) {
            IndexK expected = key_checker;
            CAS(&_[slot].key, &expected, INVALID);
        }
        if (sc) {
            *sc = Shortcut(reinterpret_cast<char *>(this), i);
        }
        old_entry->offset = old_value.offset;
        persist(&_[slot].key, sizeof(Pair));
        ret = 0;
        break;
    } else {
        LOCK = INVALID;
    }
  }

  sema.fetch_sub(1);
  return ret;
}

template <typename KeyType>
void Segment<KeyType>::Insert4split(IndexK key, IndexV value, size_t loc) {
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (loc+i) % kNumSlot;
        if (_[slot].key == INVALID) {
            _[slot].key = key;
            _[slot].value = value;
            persist(&_[slot], sizeof(Pair));
            return;
        }
    }
}

template <typename KeyType>
Segment<KeyType>** Segment<KeyType>::Split(void) {
  uint64_t lock = 0;
  if (!sema.compare_exchange_strong(lock, EXCLUSIVE_LOCK)) {
      if (lock == EXCLUSIVE_LOCK) {
          return nullptr;
      }

      lock = SPLIT_REQUEST_BIT;
      if (!sema.compare_exchange_strong(lock, EXCLUSIVE_LOCK)) {
          if ((lock & SPLIT_REQUEST_BIT) != 0) {
              return nullptr;
          }
          sema.compare_exchange_strong(lock, lock | SPLIT_REQUEST_BIT);
          return nullptr;
      }
  }

  Segment<KeyType>** split = new Segment<KeyType>*[2];
  split[0] = this;
  split[1] = new Segment<KeyType>(local_depth + 1);

  for (unsigned i = 0; i < kNumSlot; ++i) {
    size_t key_hash;
    if constexpr (using_fp_) {
        key_hash = _[i].key;
    } else {
        key_hash = h(&_[i].key, sizeof(IndexK));
    }
    if (key_hash & ((size_t) 1 << ((sizeof(IndexK)*8 - local_depth - 1)))) {
      split[1]->Insert4split(_[i].key, _[i].value, (key_hash & kMask)*kNumPairPerCacheLine);
    }
  }

    persist((char*) split[1], sizeof(Segment));
    local_depth = local_depth + 1;
    persist((char*) &local_depth, sizeof(size_t));

    return split;
}

template <typename KeyType>
CCEH<KeyType>::CCEH(size_t initCap)
    : dir{new Directory<KeyType>(static_cast<size_t>(log2(initCap)))}
{
    for (unsigned i = 0; i < dir->capacity; ++i) {
        dir->_[i] = new Segment<KeyType>(static_cast<size_t>(log2(initCap)));
        dir->_[i]->pattern = i;
    }
}

template <typename KeyType>
IndexV CCEH<KeyType>::Insert(const KeyType& key, IndexV value, Shortcut *sc) {
    return Insert(key, value, dummy_key_check, sc);
}

template <typename KeyType>
template <typename KeyCheckFn>
IndexV CCEH<KeyType>::Insert(const KeyType& key, IndexV value, KeyCheckFn key_check_fn, Shortcut *sc) {
    size_t key_hash;
    if constexpr (std::is_same_v<KeyType, std::string>) { key_hash = h(key.data(), key.length()); }
    else { key_hash = h(&key, sizeof(key)); }
    auto loc = (key_hash & kMask) * kNumPairPerCacheLine;

    while (true) {
        auto x = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
        auto target = dir->_[x];
        IndexV old_entry{};
        auto ret = target->Insert(key, value, loc, key_hash, &old_entry, key_check_fn, sc);

        if (ret == 0) {
            return old_entry;
        } else if (ret == 2) {
            continue;
        }

        // Segment is full, need to split.
        Segment<KeyType>** s = target->Split();
        if (s == nullptr) {
            // another thread is doing split
            continue;
        }

        s[0]->pattern = (key_hash >> (8 * sizeof(key_hash) - s[0]->local_depth + 1)) << 1;
        s[1]->pattern = ((key_hash >> (8 * sizeof(key_hash) - s[1]->local_depth + 1)) << 1) + 1;

        // Directory management
        while (!dir->Acquire()) {
            asm("nop");
        }

        { // CRITICAL SECTION - directory update
            x = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
            if (dir->_[x]->local_depth - 1 < dir->depth) {  // normal split
                unsigned depth_diff = dir->depth - s[0]->local_depth;
                if (depth_diff == 0) {
                    if (x % 2 == 0) {
                        dir->_[x + 1] = s[1];
                        persist((char*) &dir->_[x + 1], 8);
                    } else {
                        dir->_[x] = s[1];
                        persist((char*) &dir->_[x], 8);
                    }
                } else {
                    int chunk_size = pow(2, dir->depth - (s[0]->local_depth - 1));
                    x = x - (x % chunk_size);
                    for (unsigned i = 0; i < chunk_size / 2; ++i) {
                        dir->_[x + chunk_size / 2 + i] = s[1];
                    }
                    persist((char*) &dir->_[x + chunk_size / 2], sizeof(void*) * chunk_size / 2);
                }
                dir->Release();
            } else {  // directory doubling
                auto dir_old = dir;
                auto d = dir->_;
                auto _dir = new Directory<KeyType>(dir->depth + 1);
                for (unsigned i = 0; i < dir->capacity; ++i) {
                    if (i == x) {
                        _dir->_[2 * i] = s[0];
                        _dir->_[2 * i + 1] = s[1];
                    } else {
                        _dir->_[2 * i] = d[i];
                        _dir->_[2 * i + 1] = d[i];
                    }
                }
                persist((char*) &_dir->_[0], sizeof(Segment<KeyType>*) * _dir->capacity);
                persist((char*) &_dir, sizeof(Directory<KeyType>));
                if (!CAS(&dir, &dir_old, _dir)) {
                    throw std::runtime_error("Could not swap dirs. This should never happen!");
                }
                persist((char*) &dir, sizeof(void*));
                delete dir_old;
            }
            s[0]->sema.store(0);
        }  // End of critical section

        delete s;
    }
}

template <typename KeyType>
template <typename KeyCheckFn>
bool CCEH<KeyType>::TryGCUpdate(const KeyType& key, IndexV value, KeyCheckFn key_check_fn, IndexV* old_entry, Shortcut sc) {
    size_t key_hash;
    if constexpr (std::is_same_v<KeyType, std::string>) { key_hash = h(key.data(), key.length()); }
    else { key_hash = h(&key, sizeof(key)); }
    auto loc = (key_hash & kMask) * kNumPairPerCacheLine;

    Segment<KeyType> *target = reinterpret_cast<Segment<KeyType> *>(sc.GetNodeAddr());

    uint64_t lock = target->sema.load();
    if (lock == EXCLUSIVE_LOCK) return false;
    if (IS_BIT_SET(lock, SPLIT_REQUEST_BIT)) return false;
    const size_t pattern_shift = 8 * sizeof(key_hash) - target->local_depth;
    if ((key_hash >> pattern_shift) != target->pattern) return false;

    int ret = 1;
    while (!target->sema.compare_exchange_weak(lock, lock+1)) {
      if (lock == EXCLUSIVE_LOCK) return 2;
      if (IS_BIT_SET(lock, SPLIT_REQUEST_BIT)) return 1;
    }

    IndexK key_checker;
    if constexpr (using_fp_) {
        key_checker = key_hash;
    } else {
        key_checker = *reinterpret_cast<const IndexK*>(&key);
    }
    int idx = sc.GetPos();
    uint32_t slot = (loc + idx) % target->kNumSlot;
    if (ATOMIC_LOAD(&target->_[slot].key) == key_checker) {
        if constexpr (using_fp_) {
            // FPs matched but not necessarily the actual key.
            const bool keys_match = key_check_fn(key, target->_[slot].value);
            if (!keys_match) {
            target->sema.fetch_sub(1);
            return false;
            }
        }

        IndexV old_value = target->_[slot].value;
        while (!CAS(&target->_[slot].value.offset, &old_value.offset,
                    value.offset)) {
        }
        if (value.is_tombstone()) {
            IndexK expected = key_checker;
            CAS(&target->_[slot].key, &expected, INVALID);
        }
        old_entry->offset = old_value.offset;
        persist(&target->_[slot], sizeof(Pair));

        target->sema.fetch_sub(1);
        return true;
    }

    target->sema.fetch_sub(1);
    return false;
}


template <typename KeyType>
void CCEH<KeyType>::Remove(IndexV* offset) {
    offset_size_t expected_value = offset->offset;
    CAS(&offset->offset, &expected_value, IndexV::Tombstone().offset);
    IndexK* key_slot = reinterpret_cast<IndexK*>(offset) - 1;
    ATOMIC_STORE(key_slot, INVALID);
}

template <typename KeyType>
IndexV CCEH<KeyType>::Get(const KeyType& key) {
    return Get(key, dummy_key_check);
}

template <typename KeyType>
template <typename KeyCheckFn>
IndexV CCEH<KeyType>::Get(const KeyType& key, KeyCheckFn key_check_fn) {
    size_t key_hash;
    if constexpr (std::is_same_v<KeyType, std::string>) { key_hash = h(key.data(), key.length()); }
    else { key_hash = h(&key, sizeof(key)); }
    const size_t loc = (key_hash & kMask) * kNumPairPerCacheLine;

    Segment<KeyType>* segment;
    while (true) {
        const size_t seg_num = (key_hash >> (8 * sizeof(key_hash) - dir->depth));
        segment = dir->_[seg_num];
        auto& sema = segment->sema;

        uint64_t lock = sema.load();
        if ((lock & SPLIT_REQUEST_BIT) != 0 || !sema.compare_exchange_weak(lock, lock + 1)) {
            continue;
        }

        // Could acquire lock
        break;
    }

    IndexK key_checker;
    if constexpr (using_fp_) {
        key_checker = key_hash;
    } else {
        key_checker = *reinterpret_cast<const IndexK*>(&key);
    }

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (loc+i) % Segment<KeyType>::kNumSlot;
        if (segment->_[slot].key == key_checker) {
          if constexpr (using_fp_) {
              const bool keys_match = key_check_fn(key, segment->_[slot].value);
              if (!keys_match) continue;
          }

          IndexV offset = segment->_[slot].value;
          segment->sema.fetch_sub(1);
          return offset;
        }
    }

    segment->sema.fetch_sub(1);
    return IndexV::NONE();
}

template <typename KeyType>
size_t CCEH<KeyType>::Capacity(void) {
    std::unordered_map<Segment<KeyType>*, bool> set;
    for (size_t i = 0; i < dir->capacity; ++i) {
        set[dir->_[i]] = true;
    }
    return set.size() * Segment<KeyType>::kNumSlot;
}

template <typename KeyType>
CCEH<KeyType>::~CCEH() {
#ifndef CCEH_PERSISTENT
    // Only clean up in volatile mode
    std::unordered_map<Segment<KeyType>*, bool> set;
    for (size_t i = 0; i < dir->capacity; ++i) {
        set[dir->_[i]] = true;
    }
    for (auto const& [seg, foo] : set) {
        delete seg;
    }
#endif
}

}  // namespace cceh
}  // namespace viper
