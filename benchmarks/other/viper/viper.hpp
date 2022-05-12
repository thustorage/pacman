#pragma once

#include <assert.h>
#include <fcntl.h>
#include <immintrin.h>
#include <libpmem.h>
#include <linux/mman.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <bitset>
#include <cmath>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "cceh.hpp"
#include "concurrentqueue.h"
#include "config.h"
#include "util/util.h"
#include "util/timer.h"
#include "hotkeyset.h"

#ifndef NDEBUG
#define DEBUG_LOG(msg) (std::cout << msg << std::endl)
#else
#define DEBUG_LOG(msg) \
  do {                 \
  } while (0)
#endif

/**
 * Define this to use Viper in DRAM instead of PMem. This will allocate all
 * VPages in DRAM.
 */
#ifndef LOG_PERSISTENT
#define VIPER_DRAM
#endif

namespace viper {

using version_lock_t = uint8_t;

// static constexpr uint16_t PAGE_SIZE = 4 * 1024;  // 4kb
static constexpr uint32_t PAGE_SIZE = 16 * 1024;  // 16kb supports larger object
static constexpr uint8_t NUM_DIMMS = 3;
static constexpr size_t BLOCK_SIZE = NUM_DIMMS * PAGE_SIZE;
static constexpr size_t ONE_GB = 1024l * 1024 * 1024;

static_assert(sizeof(version_lock_t) == 1, "Lock must be 1 byte.");
static constexpr version_lock_t CLIENT_BIT = 0b10000000;
static constexpr version_lock_t NO_CLIENT_BIT = 0b01111111;
static constexpr version_lock_t USED_BIT = 0b01000000;
static constexpr version_lock_t UNLOCKED_BIT = 0b11111110;

#define IS_LOCKED(lock) ((lock)&1)

#define LINE_NUM (" (LINE: " + std::to_string(__LINE__) + ')')
#define IO_ERROR(msg)                                                        \
  throw std::runtime_error(std::string(msg) + " | " + std::strerror(errno) + \
                           LINE_NUM)
#define MMAP_CHECK(addr)                           \
  if ((addr) == nullptr || (addr) == MAP_FAILED) { \
    IO_ERROR("Cannot mmap");                       \
  }

#define LOAD_ORDER std::memory_order_acquire
#define STORE_ORDER std::memory_order_release

static constexpr auto VIPER_MAP_PROT = PROT_WRITE | PROT_READ;
static constexpr auto VIPER_MAP_FLAGS = MAP_SHARED_VALIDATE | MAP_SYNC;
static constexpr auto VIPER_DRAM_MAP_FLAGS = MAP_ANONYMOUS | MAP_PRIVATE;
static constexpr auto VIPER_FILE_OPEN_FLAGS = O_CREAT | O_RDWR | O_DIRECT;

static constexpr double MIN_RECLAIM_FREE_PERCENTAGE = 0.1;
static constexpr double MAX_RECLAIM_FREE_PERCENTAGE = 0.4;

struct ViperConfig {
  double resize_threshold = 0.85;
  double reclaim_free_percentage = 0.4;
  size_t reclaim_threshold = 1'000'000;
  uint8_t num_recovery_threads = 32;
  uint8_t num_reclaim_threads = 1;
  size_t dax_alignment = ONE_GB;
  size_t fs_alignment = ONE_GB;
  uint8_t num_client_threads = 1;
  bool enable_reclamation = true;
};

namespace internal {

template <typename K, typename V>
constexpr data_offset_size_t get_num_slots_per_page() {
  const uint32_t entry_size = sizeof(std::pair<K, V>);
  uint16_t current_page_size = PAGE_SIZE;

  // const uint16_t page_overhead = sizeof(version_lock_t) + 1;
  const uint16_t page_overhead = 8;
  while (entry_size + page_overhead > current_page_size &&
         current_page_size <= BLOCK_SIZE) {
    current_page_size += PAGE_SIZE;
  }

  page_size_t num_pages_per_block = current_page_size / PAGE_SIZE;
  if (num_pages_per_block == 4 || num_pages_per_block == 5) {
    num_pages_per_block = 6;
    current_page_size = BLOCK_SIZE;
  }

  uint16_t num_slots_per_page_large = current_page_size / entry_size;
  if (num_slots_per_page_large > 255) {
    num_slots_per_page_large--;
  }
  data_offset_size_t num_slots_per_page = num_slots_per_page_large;
  while ((num_slots_per_page * entry_size) + page_overhead +
             std::ceil((double)num_slots_per_page / 8) >
         current_page_size) {
    num_slots_per_page--;
  }
  assert(num_slots_per_page > 0 && "Cannot fit KV pair into single page!");
  return num_slots_per_page;
}

struct VarSizeEntry {
  union {
    uint64_t size_info;
    struct {
      bool is_set : 1;
      uint16_t key_size : 15;
      uint16_t value_size : 16;
      uint32_t epoch;
    };
  };
  char *data;

  VarSizeEntry(const uint64_t keySize, const uint64_t valueSize)
      : is_set{true},
        key_size{static_cast<uint16_t>(keySize)},
        value_size{static_cast<uint16_t>(valueSize)} {}
};

struct VarEntryAccessor {
  bool is_set;
  uint32_t key_size;
  uint32_t value_size;
  char *key_data = nullptr;
  char *value_data = nullptr;

  VarEntryAccessor(const char *raw_entry) {
    const VarSizeEntry *entry =
        reinterpret_cast<const VarSizeEntry *>(raw_entry);
    is_set = entry->is_set;
    key_size = entry->key_size;
    value_size = entry->value_size;
    key_data = const_cast<char *>(raw_entry) + sizeof(entry->size_info);
    value_data = key_data + key_size;
  }

  VarEntryAccessor(const char *raw_key_entry, const char *raw_value_entry)
      : VarEntryAccessor{raw_key_entry} {
    assert(value_size == 0);
    assert(key_data != nullptr);
    const VarSizeEntry *value_entry =
        reinterpret_cast<const VarSizeEntry *>(raw_value_entry);
    value_size = value_entry->value_size;
    value_data =
        const_cast<char *>(raw_value_entry) + sizeof(value_entry->size_info);
  }

  std::string_view key() { return std::string_view{key_data, key_size}; }

  std::string_view value() {
    assert(value_data != nullptr);
    return std::string_view{value_data, value_size};
  }
};

template <typename K, typename V>
struct alignas(PAGE_SIZE) ViperPage {
  using VEntry = std::pair<K, V>;
  static constexpr data_offset_size_t num_slots_per_page =
      get_num_slots_per_page<K, V>();

  std::atomic<version_lock_t> version_lock;
  std::bitset<num_slots_per_page> free_slots;

  std::array<VEntry, num_slots_per_page> data;

  void init() {
    static constexpr size_t v_page_size = sizeof(*this);
    static_assert(((v_page_size & (v_page_size - 1)) == 0),
                  "VPage needs to be a power of 2!");
    static_assert(PAGE_SIZE % alignof(*this) == 0,
                  "VPage not page size conform!");
    version_lock_t lock_value = version_lock.load(LOAD_ORDER);
    version_lock_t new_value;
    do {
      new_value = (lock_value & CLIENT_BIT) | USED_BIT;
    } while (!version_lock.compare_exchange_weak(lock_value, new_value));
    free_slots.set();
  }

  inline bool lock(const bool blocking = true) {
    version_lock_t lock_value = version_lock.load(LOAD_ORDER);
    // Compare and swap until we are the thread to set the lock bit
    lock_value &= UNLOCKED_BIT;
    while (!version_lock.compare_exchange_weak(lock_value, lock_value + 1)) {
      lock_value &= UNLOCKED_BIT;
      if (!blocking) {
        return false;
      }
    }
    return true;
  }

  inline void unlock() {
    version_lock_t current_version = version_lock.load(LOAD_ORDER);
    version_lock_t new_version;
    do {
      new_version = (current_version + 1) % USED_BIT;
      new_version |= (current_version & USED_BIT);
      new_version |= (current_version & CLIENT_BIT);

      assert((new_version & 1) == 0);
    } while (!version_lock.compare_exchange_weak(current_version, new_version));
  }

  inline void clean_used_bit() {
    version_lock_t lock_value = version_lock.load(LOAD_ORDER);
    version_lock_t new_value = lock_value & (~USED_BIT);
    while (!version_lock.compare_exchange_weak(lock_value, new_value)) {
      new_value = lock_value & (~USED_BIT);
    }
  }
};

template <>
struct alignas(PAGE_SIZE) ViperPage<std::string, std::string> {
  uint16_t next_insert_offset;
  std::atomic<version_lock_t> version_lock;
  uint32_t modified_bytes;
  uint64_t offset;
  bool has_shortcut;
  uint32_t shortcut_start_offset;
  uint64_t padding[5];

  // static constexpr size_t METADATA_SIZE = sizeof(version_lock) +
  // sizeof(next_insert_pos) + sizeof(modified_bytes);
  static constexpr size_t METADATA_SIZE = CACHE_LINE_SIZE;
  static constexpr uint16_t DATA_SIZE = PAGE_SIZE - METADATA_SIZE;
  std::array<char, DATA_SIZE> data;

  void init() {
    typedef ViperPage<std::string, std::string> Var_VPage;
    static_assert(offsetof(Var_VPage, data) == 64);
    static constexpr size_t v_page_size = sizeof(*this);
    static_assert(((v_page_size & (v_page_size - 1)) == 0),
                  "VPage needs to be a power of 2!");
    static_assert(PAGE_SIZE % alignof(*this) == 0,
                  "VPage not page size conform!");
    static_assert(PAGE_SIZE == v_page_size, "VPage not 4096 Byte!");
    version_lock_t lock_value = version_lock.load(LOAD_ORDER);
    version_lock_t new_value;
    do {
      new_value = (lock_value & CLIENT_BIT) | USED_BIT;
    } while (!version_lock.compare_exchange_weak(lock_value, new_value));
    next_insert_offset = 0;
    modified_bytes = 0;
    offset = 0;
  }

  inline bool lock(const bool blocking = true) {
    version_lock_t lock_value = version_lock.load(LOAD_ORDER);
    // Compare and swap until we are the thread to set the lock bit
    lock_value &= UNLOCKED_BIT;
    while (!version_lock.compare_exchange_weak(lock_value, lock_value + 1)) {
      lock_value &= UNLOCKED_BIT;
      if (!blocking) {
        return false;
      }
    }
    return true;
  }

  inline void unlock() {
    version_lock_t current_version = version_lock.load(LOAD_ORDER);
    version_lock_t new_version;
    do {
      new_version = (current_version + 1) % USED_BIT;
      new_version |= (current_version & USED_BIT);
      new_version |= (current_version & CLIENT_BIT);
    } while (!version_lock.compare_exchange_weak(current_version, new_version));
  }

  inline void clean_used_bit() {
    version_lock_t lock_value = version_lock.load(LOAD_ORDER);
    version_lock_t new_value;
    do {
      new_value = lock_value & (~USED_BIT);
    } while (!version_lock.compare_exchange_weak(lock_value, new_value));
    modified_bytes = 0;
  }
};

template <typename VPage, page_size_t num_pages>
struct alignas(PAGE_SIZE) ViperPageBlock {
  /**
   * Array to store all persistent ViperPages.
   * Don't use a vector here because a ViperPage uses arrays and the whole
   * struct would be moved on a vector resize, making all pointers invalid.
   */
  std::array<VPage, num_pages> v_pages;

  inline bool is_owned() const {
    return IS_BIT_SET(v_pages[0].version_lock, CLIENT_BIT);
  }

  inline bool is_unused() const {
    for (const VPage &v_page : v_pages) {
      if (IS_BIT_SET(v_page.version_lock, USED_BIT)) return false;
    }
    return true;
  }

  inline bool get_ownership() {
    version_lock_t lock_value = v_pages[0].version_lock.load(LOAD_ORDER);
    while (!IS_BIT_SET(lock_value, CLIENT_BIT)) {
      if (v_pages[0].version_lock.compare_exchange_weak(
              lock_value, lock_value | CLIENT_BIT)) {
        return true;
      }
    }
    return false;
  }

  inline void release_ownership() {
    version_lock_t lock_value = v_pages[0].version_lock.load(LOAD_ORDER);
    while (!v_pages[0].version_lock.compare_exchange_weak(
        lock_value, lock_value & NO_CLIENT_BIT))
      ;
  }

  inline void clean_version_lock() {
    v_pages[0].lock();
    v_pages[0].version_lock = 0;
  }
};

}  // namespace internal

struct ViperFileMetadata {
  const size_t block_offset;
  const size_t block_size;
  const size_t alloc_size;
  std::atomic<block_size_t> num_used_blocks;
  block_size_t num_allocated_blocks;
  size_t total_mapped_size;
};

struct ViperFileMapping {
  const size_t mapped_size;
  void *const start_addr;
};

struct ViperBase {
  const int file_descriptor;
  const bool is_new_db;
  const bool is_file_based;
  ViperFileMetadata *const v_metadata;
  std::vector<ViperFileMapping> v_mappings;
};

struct ViperInitData {
  int fd;
  ViperFileMetadata *meta;
  std::vector<ViperFileMapping> mappings;
};

template <typename KeyT>
struct KeyAccessor {
  typedef const KeyT *checker_type;
};

template <>
struct KeyAccessor<std::string> {
  typedef std::string_view checker_type;
};

template <typename ValueT>
struct ValueAccessor {
  typedef ValueT *type;
  typedef const ValueT *const_type;
  typedef ValueT *ptr_type;
  typedef const ValueT *const_ptr_type;
  typedef const ValueT &ref_type;
  typedef const ValueT *checker_type;

  static ptr_type to_ptr_type(type &x) { return x; }
  static const_ptr_type to_ptr_type(const type &x) { return x; }
};

template <>
struct ValueAccessor<std::string> {
  typedef std::string_view type;
  typedef const std::string_view const_type;
  typedef std::string_view *ptr_type;
  typedef const std::string_view *const_ptr_type;
  typedef const std::string_view &ref_type;
  typedef const std::string_view checker_type;

  static ptr_type to_ptr_type(type &x) { return &x; }
  static const_ptr_type to_ptr_type(const type &x) { return &x; }
};

template <typename K, typename V>
class Viper {
  using ViperT = Viper<K, V>;
  using VPage = internal::ViperPage<K, V>;
  using KVOffset = KeyValueOffset;
  static constexpr uint64_t v_page_size = sizeof(VPage);
  static_assert(BLOCK_SIZE % v_page_size == 0, "Page needs to fit into block.");
  static constexpr page_size_t num_pages_per_block = BLOCK_SIZE / v_page_size;
  using VPageBlock = internal::ViperPageBlock<VPage, num_pages_per_block>;

 public:
  static std::unique_ptr<Viper<K, V>> create(
      const std::string &pool_file, uint64_t initial_pool_size,
      ViperConfig v_config = ViperConfig{});
  static std::unique_ptr<Viper<K, V>> open(
      const std::string &pool_file, ViperConfig v_config = ViperConfig{});
  Viper(ViperBase v_base, std::filesystem::path pool_dir, bool owns_pool,
        ViperConfig v_config);
  ~Viper();
  void StopReclaim();

  class ReadOnlyClient {
    friend class Viper<K, V>;

   public:
    bool get(const K &key, V *value) const;
    size_t get_total_used_pmem() const;
    size_t get_total_allocated_pmem() const;

   protected:
    explicit ReadOnlyClient(ViperT &viper);
    inline const std::pair<typename KeyAccessor<K>::checker_type,
                           typename ValueAccessor<V>::checker_type>
    get_const_entry_from_offset(KVOffset offset) const;
    inline bool get_const_value_from_offset(KVOffset offset, V *value) const;
    ViperT &viper_;
  };

  class Client : public ReadOnlyClient {
    friend class Viper<K, V>;
    friend class ClientWrapper;

   public:
    bool put(const K &key, const V &value);

    bool get(const K &key, V *value);
    bool get(const K &key, V *value) const;

    template <typename UpdateFn>
    bool update(const K &key, UpdateFn update_fn);

    bool remove(const K &key);

    Client(ViperT &viper);
    ~Client();

    bool is_reclaiming_client_ = false;
    bool is_cold_client_ = false;
    bool has_shortcut_ = false;
    std::vector<Shortcut> shortcut_buffer_;
#ifdef LOGGING
    uint64_t num_shortcut_succeed_;
    uint64_t num_total_put_;
#endif
    int client_id_;
    block_size_t used_blocks = 0;

#ifdef BATCH_COMPACTION
    VPage *volatile_page_ = nullptr;

    struct ValidKVInfo {
      ValidKVInfo(const std::string &k, const KVOffset &old_offset,
                  const KVOffset &new_offset, Shortcut &sc)
          : key(k), old_offset(old_offset), new_offset(new_offset), sc(sc) {}
      K key;
      KVOffset old_offset;
      KVOffset new_offset;
      Shortcut sc;
    };
    std::vector<ValidKVInfo> to_insert_offset_;
#endif

   protected:
    bool put(const K &key, const V &value, bool delete_old, const KVOffset gc_old_offset = KVOffset::Tombstone(), Shortcut sc = Shortcut());
#ifdef BATCH_COMPACTION
    void FlushAndInsertOffsets();
    void ReclaimBatchPut(const K &key, const V &value,
                         const KVOffset old_offset, Shortcut sc);
#endif
    inline void update_access_information();
    inline void update_var_size_page_information();
    inline bool get_value_from_offset(KVOffset offset, V *value);
    inline void info_sync(bool force = false);
    void free_occupied_slot(const KVOffset offset_to_delete, const K &key,
                            const bool delete_offset = false);
    void invalidate_record(VPage *v_page, const KVOffset kv_offset);

    enum PageStrategy : uint8_t { BlockBased, DimmBased };

    PageStrategy strategy_;

    // Fixed size entries
    block_size_t v_block_number_;
    page_size_t v_page_number_;
    VPageBlock *v_block_;
    VPage *v_page_;

    // Dimm-based
    block_size_t end_v_block_number_;

    // Block-based
    page_size_t num_v_pages_processed_;

    uint16_t op_count_;
    size_t num_reclaimable_ops_;
    int size_delta_;
#ifdef LOGGING
    uint64_t update_index_time_;
#endif
  };

  class ClientWrapper {
    friend class Viper<K, V>;

   public:
    bool put(const K &key, const V &value);
    bool get(const K &key, V *value) const;

    template <typename UpdateFn>
    bool update(const K &key, UpdateFn update_fn);

    bool remove(const K &key);

    ClientWrapper(ViperT &viper);
    ~ClientWrapper();

    int client_wrapper_id_;

   protected:
    ViperT &viper_;
    Client *client_ = nullptr;
#ifdef HOT_COLD_SEPARATE
    Client *cold_client_ = nullptr;
#endif
  };

  static constexpr uint32_t BYTES_PER_BIT = 32;
  static constexpr uint32_t BITMAP_SIZE_PAGE =
      (PAGE_SIZE / BYTES_PER_BIT + 7) / 8;
  struct GCInfo {
    // std::atomic<version_lock_t> version_lock[NUM_DIMMS];
    uint32_t modified_bytes[NUM_DIMMS];
    std::array<std::array<uint8_t, BITMAP_SIZE_PAGE>, NUM_DIMMS> garbage_bitmap;

    GCInfo() {
      for (int i = 0; i < NUM_DIMMS; i++) {
        // version_lock[i].store(0);
        Clear(i);
      }
    }

    void Clear(int page_number) {
      modified_bytes[page_number] = 0;
      memset(garbage_bitmap[page_number].data(), 0, BITMAP_SIZE_PAGE);
    }

    bool IsGarbage(int page_number, uint32_t offset) {
      assert(offset < PAGE_SIZE);
      int idx = offset / BYTES_PER_BIT;
      int byte = idx / 8;
      int bit = idx % 8;
      return (garbage_bitmap[page_number][byte] >> bit) & 1;
    }

    void SetGarbage(int page_number, uint32_t offset) {
      int idx = offset / BYTES_PER_BIT;
      int byte = idx / 8;
      int bit = idx % 8;
      uint8_t old_val = garbage_bitmap[page_number][byte];
      while (true) {
        assert(((old_val >> bit) & 1) == 0);
        uint8_t new_val = old_val | (1 << bit);
        if (__atomic_compare_exchange_n(&garbage_bitmap[page_number][byte],
                                        &old_val, new_val, true,
                                        __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
          break;
        }
      }
    }

    // page:
    // void init(int pid) {
    //   version_lock_t lock_value = version_lock[pid].load(LOAD_ORDER);
    //   version_lock_t new_value;
    //   do {
    //     new_value = (lock_value & CLIENT_BIT) | USED_BIT;
    //   } while (!version_lock[pid].compare_exchange_weak(lock_value, new_value));
    //   modified_bytes = 0;
    // }

    // inline bool lock(int pid, const bool blocking = true) {
    //   version_lock_t lock_value = version_lock[pid].load(LOAD_ORDER);
    //   // Compare and swap until we are the thread to set the lock bit
    //   lock_value &= UNLOCKED_BIT;
    //   while (!version_lock[pid].compare_exchange_weak(lock_value,
    //                                                   lock_value + 1)) {
    //     lock_value &= UNLOCKED_BIT;
    //     if (!blocking) {
    //       return false;
    //     }
    //   }
    //   return true;
    // }

    // inline void unlock(int pid) {
    //   version_lock_t current_version = version_lock[pid].load(LOAD_ORDER);
    //   version_lock_t new_version;
    //   do {
    //     new_version = (current_version + 1) % USED_BIT;
    //     new_version |= (current_version & USED_BIT);
    //     new_version |= (current_version & CLIENT_BIT);
    //   } while (!version_lock[pid].compare_exchange_weak(current_version,
    //                                                     new_version));
    // }

    // inline void clean_used_bit(int pid) {
    //   version_lock_t lock_value = version_lock[pid].load(LOAD_ORDER);
    //   version_lock_t new_value;
    //   do {
    //     new_value = lock_value & (~USED_BIT);
    //   } while (!version_lock[pid].compare_exchange_weak(lock_value, new_value));
    //   modified_bytes = 0;
    // }

    // block:
    // inline bool is_owned() const {
    //   return IS_BIT_SET(version_lock[0], CLIENT_BIT);
    // }

    // inline bool is_unused() const {
    //   for (int i = 0; i < NUM_DIMMS; ++i) {
    //     if (IS_BIT_SET(version_lock[0], USED_BIT)) return false;
    //   }
    //   return true;
    // }

    // inline bool get_ownership() {
    //   version_lock_t lock_value = version_lock[0].load(LOAD_ORDER);
    //   while (!IS_BIT_SET(lock_value, CLIENT_BIT)) {
    //     if (version_lock[0].compare_exchange_weak(
    //             lock_value, lock_value | CLIENT_BIT)) {
    //       return true;
    //     }
    //   }
    //   return false;
    // }

    // inline void release_ownership() {
    //   version_lock_t lock_value = version_lock[0].load(LOAD_ORDER);
    //   while (!version_lock[0].compare_exchange_weak(
    //       lock_value, lock_value & NO_CLIENT_BIT))
    //     ;
    // }

    // inline void clean_version_lock() {
    //   lock(0);
    //   version_lock[0] = 0;
    // }
  };

  // void reclaim();
  // void reclaim_work_t(int id);
  void reclaim(int id);
  void reclaim_work(int id, Client &client);
  void StartCleanStatistics();
  double GetCompactionCPUUsage();
  double GetReclaimThroughput();

  Client get_client();
  Client get_reclaim_client();
  std::unique_ptr<ClientWrapper> get_client_ptr();
  ReadOnlyClient get_read_only_client();

 protected:
  static ViperBase init_pool(const std::string &pool_file, uint64_t pool_size,
                             bool is_new_pool, ViperConfig v_config);

  void get_new_access_information(Client *client);
  void get_block_based_access(Client *client);
  void get_new_var_size_access_information(Client *client);
  KVOffset get_new_block();
  void remove_client(Client *client);

  ViperFileMapping allocate_v_page_blocks();
  void add_v_page_blocks(ViperFileMapping mapping);
  void recover_database();
  void trigger_resize();
  void trigger_reclaim(size_t num_reclaim_ops);
  void compact(Client &client, VPageBlock *v_block, block_size_t block_number);

  bool check_key_equality(const K &key, const KVOffset offset_to_compare);

  ViperBase v_base_;
  const bool owns_pool_;
  ViperConfig v_config_;
  std::filesystem::path pool_dir_;

  cceh::CCEH<K> map_;
  static constexpr bool using_fp = requires_fingerprint(K);

  std::vector<VPageBlock *> v_blocks_;
  std::atomic<size_t> num_v_blocks_;
  std::atomic<size_t> current_size_;
  std::atomic<size_t> reclaimable_ops_;
  std::atomic<offset_size_t> current_block_page_;
  moodycamel::ConcurrentQueue<block_size_t> free_blocks_;

#ifdef REDUCE_PM_ACCESS
  std::vector<GCInfo> block_gcinfo_;
#endif

#ifdef HOT_COLD_SEPARATE
  HotKeySet *hot_key_set_;
#endif

  const double resize_threshold_;
  std::atomic<bool> is_resizing_;
  std::unique_ptr<std::thread> resize_thread_;
  std::atomic<bool> is_v_blocks_resizing_;

  const size_t reclaim_threshold_;
  std::atomic<bool> is_reclaiming_;
  std::unique_ptr<std::thread[]> reclaim_thread_;
  // std::unique_ptr<std::thread> reclaim_thread_;

  std::atomic<bool> deadlock_offset_lock_;
  std::vector<KVOffset> deadlock_offsets_;

  std::atomic<uint8_t> num_active_clients_;
  const uint8_t num_recovery_threads_;
  const uint8_t num_reclaim_threads_;
  std::atomic<double> reclaim_percentage_;
  std::atomic<uint64_t> reclaimed_blocks_ = 0;
  uint64_t last_reclaimed_blocks_ = 0;
  std::unique_ptr<std::atomic_uint64_t[]> local_reclaim_blocks_;
  std::unique_ptr<uint64_t[]> local_reclaim_blocks_before_;
  std::unique_ptr<std::atomic_uint64_t[]> local_reclaim_time_ns_;
  std::unique_ptr<uint64_t[]> local_reclaim_time_before_ns_;
  uint64_t start_clean_statistics_time = 0;
  std::mutex reserved_blocks_lock_;
  std::queue<block_size_t> reserved_blocks_;
  std::atomic<bool> do_reclaim_{false};
  std::atomic<uint64_t> reclaim_epoch_{0};
  std::atomic<int> reclaim_num_done_{0};
};

template <typename K, typename V>
std::unique_ptr<Viper<K, V>> Viper<K, V>::create(const std::string &pool_file,
                                                 uint64_t initial_pool_size,
                                                 ViperConfig v_config) {
  return std::make_unique<Viper<K, V>>(
      init_pool(pool_file, initial_pool_size, true, v_config), pool_file, true,
      v_config);
}

template <typename K, typename V>
std::unique_ptr<Viper<K, V>> Viper<K, V>::open(const std::string &pool_file,
                                               ViperConfig v_config) {
  return std::make_unique<Viper<K, V>>(init_pool(pool_file, 0, false, v_config),
                                       pool_file, true, v_config);
}

template <typename K, typename V>
Viper<K, V>::Viper(ViperBase v_base, const std::filesystem::path pool_dir,
                   const bool owns_pool, const ViperConfig v_config)
    : v_base_{v_base},
      map_{131072},
      owns_pool_{owns_pool},
      v_config_{v_config},
      pool_dir_{pool_dir},
      resize_threshold_{v_config.resize_threshold},
      reclaim_threshold_{v_config.reclaim_threshold},
      num_recovery_threads_{v_config.num_recovery_threads},
      num_reclaim_threads_{v_config.num_reclaim_threads} {
  current_block_page_ = 0;
  current_size_ = 0;
  reclaimable_ops_ = 0;
  is_resizing_ = false;
  is_reclaiming_ = false;
  num_active_clients_ = 0;
  deadlock_offset_lock_ = false;
  reclaim_percentage_.store(v_config_.reclaim_free_percentage);

  std::srand(std::time(nullptr));

  if (v_base_.v_mappings.empty()) {
    throw new std::runtime_error(
        "Need to have at least one memory section mapped.");
  }

  for (ViperFileMapping mapping : v_base_.v_mappings) {
    add_v_page_blocks(mapping);
  }

  if (!v_base_.is_new_db) {
    DEBUG_LOG("Recovering existing database.");
    recover_database();
  }
  current_block_page_ =
      KVOffset{v_base.v_metadata->num_used_blocks.load(LOAD_ORDER), 0, 0}
          .offset;

  if (v_config_.enable_reclamation) {
    do_reclaim_.store(true, STORE_ORDER);
    local_reclaim_time_ns_ =
        std::make_unique<std::atomic_uint64_t[]>(num_reclaim_threads_);
    local_reclaim_time_before_ns_ =
        std::make_unique<std::uint64_t[]>(num_reclaim_threads_);
    local_reclaim_blocks_ =
        std::make_unique<std::atomic_uint64_t[]>(num_reclaim_threads_);
    local_reclaim_blocks_before_ =
        std::make_unique<std::uint64_t[]>(num_reclaim_threads_);
    reclaim_thread_ = std::make_unique<std::thread[]>(num_reclaim_threads_);
    for (int i = 0; i < num_reclaim_threads_; ++i) {
      local_reclaim_time_ns_[i] = 0;
      local_reclaim_time_before_ns_[i] = 0;
      local_reclaim_blocks_[i] = 0;
      local_reclaim_blocks_before_[i] = 0;
      reclaim_thread_[i] = std::thread(&Viper<K, V>::reclaim, this, i);
    }
  }

#ifdef HOT_COLD_SEPARATE
  hot_key_set_ = new HotKeySet(v_config_.num_client_threads);
#endif
}

template <typename K, typename V>
Viper<K, V>::~Viper() {
  StopReclaim();
  if (num_active_clients_ != 0) {
    printf("still %d alive clients\n", num_active_clients_.load());
  }
  if (owns_pool_) {
    DEBUG_LOG("Closing pool file.");
    munmap(v_base_.v_metadata, v_base_.v_metadata->block_offset);
    for (const ViperFileMapping &mapping : v_base_.v_mappings) {
      munmap(mapping.start_addr, mapping.mapped_size);
    }
    close(v_base_.file_descriptor);
  }
#ifdef HOT_COLD_SEPARATE
  delete hot_key_set_;
  hot_key_set_ = nullptr;
#endif
}

template <typename K, typename V>
void Viper<K, V>::StopReclaim() {
  if (v_config_.enable_reclamation) {
    do_reclaim_.store(false, STORE_ORDER);
    if (reclaim_thread_) {
      for (int i = 0; i < num_reclaim_threads_; i++) {
        if (reclaim_thread_[i].joinable()) {
          reclaim_thread_[i].join();
        }
      }
      reclaim_thread_.reset();
    }
  }
}

ViperInitData init_dram_pool(uint64_t pool_size, ViperConfig v_config,
                             const size_t block_size) {
  std::cout << "Running Viper completely in DRAM." << std::endl;
  const size_t alloc_size = pool_size;
  const size_t num_alloc_chunks = pool_size / alloc_size;
  if (num_alloc_chunks == 0) {
    throw std::runtime_error("Pool too small: " + std::to_string(pool_size));
  }

  void *pmem_addr =
      mmap(nullptr, pool_size, VIPER_MAP_PROT, VIPER_DRAM_MAP_FLAGS, -1, 0);
  MMAP_CHECK(pmem_addr)

  ViperFileMetadata v_metadata{.block_offset = PAGE_SIZE,
                               .block_size = block_size,
                               .alloc_size = alloc_size,
                               .num_used_blocks = 0,
                               .num_allocated_blocks = 0,
                               .total_mapped_size = pool_size};

  ViperFileMetadata *metadata = static_cast<ViperFileMetadata *>(pmem_addr);
  memcpy(metadata, &v_metadata, sizeof(v_metadata));

  char *map_start_addr = (char *)pmem_addr;
  // Each alloc chunk gets its own mapping.
  std::vector<ViperFileMapping> mappings;
  mappings.reserve(num_alloc_chunks);

  // First chunk is special because of the initial metadata chunk
  ViperFileMapping initial_mapping{
      .mapped_size = alloc_size - metadata->block_offset,
      .start_addr = map_start_addr + metadata->block_offset};
  mappings.push_back(initial_mapping);
  block_size_t num_allocated_blocks = initial_mapping.mapped_size / block_size;

  // Remaining chunks are all the same
  for (size_t alloc_chunk = 1; alloc_chunk < num_alloc_chunks; ++alloc_chunk) {
    char *alloc_chunk_start = map_start_addr + (alloc_chunk * alloc_size);
    ViperFileMapping mapping{.mapped_size = alloc_size,
                             .start_addr = alloc_chunk_start};
    mappings.push_back(mapping);
  }
  num_allocated_blocks += (num_alloc_chunks - 1) * (alloc_size / block_size);

  // Update num allocated blocks with correct counting of allocation chunks.
  metadata->num_allocated_blocks = num_allocated_blocks;
  pmem_persist(metadata, sizeof(ViperFileMetadata));
  return ViperInitData{
      .fd = -1, .meta = metadata, .mappings = std::move(mappings)};
}

ViperInitData init_devdax_pool(const std::string &pool_file, uint64_t pool_size,
                               bool is_new_pool, ViperConfig v_config,
                               const size_t block_size) {
  const int fd = ::open(pool_file.c_str(), O_RDWR);
  if (fd < 0) {
    IO_ERROR("Cannot open dax device: " + pool_file);
  }

  size_t alloc_size = v_config.dax_alignment;
  if (!is_new_pool) {
    const size_t meta_map_size = alloc_size;
    void *metadata_addr =
        mmap(nullptr, meta_map_size, PROT_READ, MAP_SHARED, fd, 0);
    if (metadata_addr == nullptr) {
      IO_ERROR("Cannot mmap pool file: " + pool_file);
    }
    const ViperFileMetadata *metadata =
        static_cast<ViperFileMetadata *>(metadata_addr);
    if (metadata->num_allocated_blocks > 0 && metadata->block_size > 0) {
      pool_size = metadata->total_mapped_size;
      alloc_size = metadata->alloc_size;
    } else {
      DEBUG_LOG("Opening empty database. Creating new one instead.");
      is_new_pool = true;
    }
    munmap(metadata_addr, meta_map_size);
  }

  if (pool_size % alloc_size != 0) {
    throw std::runtime_error("Pool needs to be allocated in 1 GB chunks.");
  }

  const size_t num_alloc_chunks = pool_size / alloc_size;
  if (num_alloc_chunks == 0) {
    throw std::runtime_error("Pool too small: " + std::to_string(pool_size));
  }

  void *pmem_addr =
      mmap(nullptr, pool_size, VIPER_MAP_PROT, VIPER_MAP_FLAGS, fd, 0);
  MMAP_CHECK(pmem_addr)

  if (is_new_pool) {
    ViperFileMetadata v_metadata{.block_offset = PAGE_SIZE,
                                 .block_size = block_size,
                                 .alloc_size = alloc_size,
                                 .num_used_blocks = 0,
                                 .num_allocated_blocks = 0,
                                 .total_mapped_size = pool_size};
    pmem_memcpy_persist(pmem_addr, &v_metadata, sizeof(v_metadata));
  }
  ViperFileMetadata *metadata = static_cast<ViperFileMetadata *>(pmem_addr);
  char *map_start_addr = (char *)pmem_addr;

  // Each alloc chunk gets its own mapping.
  std::vector<ViperFileMapping> mappings;
  mappings.reserve(num_alloc_chunks);

  // First chunk is special because of the initial metadata chunk
  ViperFileMapping initial_mapping{
      .mapped_size = alloc_size - metadata->block_offset,
      .start_addr = map_start_addr + metadata->block_offset};
  mappings.push_back(initial_mapping);
  block_size_t num_allocated_blocks = initial_mapping.mapped_size / block_size;

  // Remaining chunks are all the same
  for (size_t alloc_chunk = 1; alloc_chunk < num_alloc_chunks; ++alloc_chunk) {
    char *alloc_chunk_start = map_start_addr + (alloc_chunk * alloc_size);
    ViperFileMapping mapping{.mapped_size = alloc_size,
                             .start_addr = alloc_chunk_start};
    mappings.push_back(mapping);
  }
  num_allocated_blocks += (num_alloc_chunks - 1) * (alloc_size / block_size);

  // Update num allocated blocks with correct counting of allocation chunks.
  metadata->num_allocated_blocks = num_allocated_blocks;
  pmem_persist(metadata, sizeof(ViperFileMetadata));
  return ViperInitData{
      .fd = fd, .meta = metadata, .mappings = std::move(mappings)};
}

ViperInitData init_file_pool(const std::string &pool_dir, uint64_t pool_size,
                             bool is_new_pool, ViperConfig v_config,
                             const size_t block_size) {
  // if (is_new_pool && std::filesystem::exists(pool_dir) &&
  // !std::filesystem::is_empty(pool_dir)) {
  //     throw std::runtime_error("Cannot create new database in non-empty
  //     directory");
  // }

  std::filesystem::create_directory(pool_dir);
  const std::filesystem::path meta_file = pool_dir + "/meta";
  ViperFileMetadata *metadata;
  const int meta_fd = ::open(meta_file.c_str(), VIPER_FILE_OPEN_FLAGS, 0644);
  if (meta_fd < 0) {
    IO_ERROR("Cannot open meta file: " + meta_file.string());
  }
  // size_t alloc_size = v_config.fs_alignment;
  size_t alloc_size = pool_size;
  if (is_new_pool) {
    if (ftruncate(meta_fd, PAGE_SIZE) != 0) {
      IO_ERROR("Could not truncate: " + meta_file.string());
    }
  } else {
    const size_t meta_map_size = alloc_size;
    void *metadata_addr =
        mmap(nullptr, PAGE_SIZE, VIPER_MAP_PROT, VIPER_MAP_FLAGS, meta_fd, 0);
    MMAP_CHECK(metadata_addr)

    metadata = static_cast<ViperFileMetadata *>(metadata_addr);
    if (metadata->num_allocated_blocks > 0 && metadata->block_size > 0) {
      pool_size = metadata->total_mapped_size;
      alloc_size = metadata->alloc_size;
    } else {
      throw std::runtime_error("Bas existing database");
    }
  }

  if (pool_size % block_size != 0) {
    throw std::runtime_error(
        "Pool needs to be allocated in block_size chunks.");
  }

  const size_t num_alloc_chunks = pool_size / alloc_size;
  if (num_alloc_chunks == 0) {
    throw std::runtime_error("Pool too small: " + std::to_string(pool_size));
  }

  // Each alloc chunk gets its own mapping.
  std::vector<ViperFileMapping> mappings;
  mappings.reserve(num_alloc_chunks);

  for (size_t chunk_num = 0; chunk_num < num_alloc_chunks; ++chunk_num) {
    std::filesystem::path data_file =
        pool_dir + "/data" + std::to_string(chunk_num);
    const int data_fd = ::open(data_file.c_str(), VIPER_FILE_OPEN_FLAGS, 0644);
    if (data_fd < 0) {
      IO_ERROR("Cannot open data file: " + data_file.string());
    }
    if (is_new_pool) {
      if (fallocate(data_fd, 0, 0, alloc_size) != 0) {
        IO_ERROR("Could not allocate: " + data_file.string());
      }
    }

    void *pmem_addr =
        mmap(nullptr, alloc_size, VIPER_MAP_PROT, VIPER_MAP_FLAGS, data_fd, 0);
    MMAP_CHECK(pmem_addr)
    ViperFileMapping mapping{.mapped_size = alloc_size,
                             .start_addr = (char *)pmem_addr};
    mappings.push_back(mapping);
    ::close(data_fd);
  }

  const size_t num_allocated_blocks =
      num_alloc_chunks * (alloc_size / block_size);

  printf("pool size %lu MB, num_allocated_blocks %lu\n", pool_size >> 20,
         num_allocated_blocks);

  if (is_new_pool) {
    void *metadata_addr =
        mmap(nullptr, alloc_size, VIPER_MAP_PROT, VIPER_MAP_FLAGS, meta_fd, 0);
    MMAP_CHECK(metadata_addr)
    ViperFileMetadata v_metadata{.block_offset = PAGE_SIZE,
                                 .block_size = block_size,
                                 .alloc_size = alloc_size,
                                 .num_used_blocks = 0,
                                 .num_allocated_blocks = num_allocated_blocks,
                                 .total_mapped_size = pool_size};
    pmem_memcpy_persist(metadata_addr, &v_metadata, sizeof(v_metadata));
    metadata = static_cast<ViperFileMetadata *>(metadata_addr);
  }
  ::close(meta_fd);
  return ViperInitData{
      .fd = -1, .meta = metadata, .mappings = std::move(mappings)};
}

template <typename K, typename V>
ViperBase Viper<K, V>::init_pool(const std::string &pool_file,
                                 uint64_t pool_size, bool is_new_pool,
                                 ViperConfig v_config) {
  constexpr size_t block_size = sizeof(VPageBlock);
  ViperInitData init_data;

  const auto start = std::chrono::steady_clock::now();

#ifdef VIPER_DRAM
  const bool is_file_based = false;
  init_data = init_dram_pool(pool_size, v_config, block_size);
#else
  DEBUG_LOG((is_new_pool ? "Creating" : "Opening")
            << " pool file " << pool_file);
  const bool is_file_based = pool_file.find("/dev/dax") == std::string::npos;
  if (is_file_based) {
    init_data =
        init_file_pool(pool_file, pool_size, is_new_pool, v_config, block_size);
  } else {
    init_data = init_devdax_pool(pool_file, pool_size, is_new_pool, v_config,
                                 block_size);
  }
#endif

  const auto end = std::chrono::steady_clock::now();
  DEBUG_LOG((is_new_pool ? "Creating" : "Opening")
            << " took " << ((end - start).count() / 1e6) << " ms");
  return ViperBase{.file_descriptor = init_data.fd,
                   .is_new_db = is_new_pool,
                   .is_file_based = is_file_based,
                   .v_metadata = init_data.meta,
                   .v_mappings = std::move(init_data.mappings)};
}

template <typename K, typename V>
ViperFileMapping Viper<K, V>::allocate_v_page_blocks() {
  const size_t alloc_size = v_base_.v_metadata->alloc_size;

  void *pmem_addr;
#ifdef VIPER_DRAM
  pmem_addr =
      mmap(nullptr, alloc_size, VIPER_MAP_PROT, VIPER_DRAM_MAP_FLAGS, -1, 0);
#else
  if (v_base_.is_file_based) {
    size_t next_file_id = v_base_.v_metadata->total_mapped_size / alloc_size;
    std::filesystem::path data_file =
        pool_dir_ / ("data" + std::to_string(next_file_id));
    DEBUG_LOG("Added data file " << data_file);
    const int data_fd = ::open(data_file.c_str(), VIPER_FILE_OPEN_FLAGS, 0644);
    if (data_fd < 0) {
      IO_ERROR("Cannot open meta file: " + data_file.string());
    }
    if (fallocate(data_fd, 0, 0, alloc_size) != 0) {
      IO_ERROR("Could not allocate: " + data_file.string());
    }
    pmem_addr =
        mmap(nullptr, alloc_size, VIPER_MAP_PROT, VIPER_MAP_FLAGS, data_fd, 0);
    ::close(data_fd);
  } else {
    const size_t offset = v_base_.v_metadata->total_mapped_size;
    const int fd = v_base_.file_descriptor;
    pmem_addr =
        mmap(nullptr, alloc_size, VIPER_MAP_PROT, VIPER_MAP_FLAGS, fd, offset);
  }
#endif

  MMAP_CHECK(pmem_addr)
  const block_size_t num_blocks_to_map = alloc_size / sizeof(VPageBlock);
  v_base_.v_metadata->num_allocated_blocks += num_blocks_to_map;
  v_base_.v_metadata->total_mapped_size += alloc_size;
  pmem_persist(v_base_.v_metadata, sizeof(ViperFileMetadata));
  DEBUG_LOG("Allocated " << num_blocks_to_map << " blocks in "
                         << (alloc_size / ONE_GB) << " GiB.");

  ViperFileMapping mapping{alloc_size, pmem_addr};
  v_base_.v_mappings.push_back(mapping);
  return mapping;
}

template <typename K, typename V>
void Viper<K, V>::add_v_page_blocks(ViperFileMapping mapping) {
  VPageBlock *start_block = reinterpret_cast<VPageBlock *>(mapping.start_addr);
  const block_size_t num_blocks_to_map =
      mapping.mapped_size / sizeof(VPageBlock);

  int num_reclaim_reserved = v_config_.num_reclaim_threads;
  is_v_blocks_resizing_.store(true, STORE_ORDER);
  v_blocks_.reserve(v_blocks_.size() + num_blocks_to_map);
  is_v_blocks_resizing_.store(false, STORE_ORDER);
  for (block_size_t block_offset = 0; block_offset < num_blocks_to_map;
       ++block_offset) {
    v_blocks_.push_back(start_block + block_offset);
  }
  for (int i = 0; i < num_reclaim_reserved; i++) {
    reserved_blocks_.push(num_blocks_to_map - 1 - i);
  }
  num_v_blocks_.store(v_blocks_.size(), STORE_ORDER);
#ifdef REDUCE_PM_ACCESS
  block_gcinfo_.resize(v_blocks_.size());
#endif
}

template <typename K, typename V>
void Viper<K, V>::recover_database() {
  auto start = std::chrono::steady_clock::now();

  const block_size_t num_used_blocks =
      v_base_.v_metadata->num_used_blocks.load(LOAD_ORDER);
  DEBUG_LOG("Re-inserting values from " << num_used_blocks << " block(s).");
  const size_t num_rec_threads =
      std::min(num_used_blocks, (size_t)num_recovery_threads_);

  std::vector<std::thread> recovery_threads;
  recovery_threads.reserve(num_rec_threads);

  auto key_check_fn = [&](auto key, auto offset) {
    return check_key_equality(key, offset);
  };

  auto recover = [&](const size_t thread_num, const block_size_t start_block,
                     const block_size_t end_block) {
    size_t num_entries = 0;
    for (block_size_t block_num = start_block; block_num < end_block;
         ++block_num) {
      VPageBlock *block = v_blocks_[block_num];
      for (page_size_t page_num = 0; page_num < num_pages_per_block;
           ++page_num) {
        const VPage &page = block->v_pages[page_num];
        if (!IS_BIT_SET(page.version_lock, USED_BIT)) {
          // Page is empty
          continue;
        }
        for (data_offset_size_t slot_num = 0;
             slot_num < VPage::num_slots_per_page; ++slot_num) {
          if (page.free_slots[slot_num]) {
            // No data, continue
            continue;
          }

          // Data is present
          const K &key = page.data[slot_num].first;
          const KVOffset offset{block_num, page_num, slot_num};
          map_.Insert(key, offset, key_check_fn);
          num_entries++;
        }
      }
    }
    current_size_.fetch_add(num_entries);
  };

  // We give each thread + 1 blocks to avoid leaving out blocks at the end.
  const block_size_t num_blocks_per_thread =
      (num_used_blocks / num_rec_threads) + 1;

  for (size_t thread_num = 0; thread_num < num_rec_threads; ++thread_num) {
    const block_size_t start_block = thread_num * num_blocks_per_thread;
    const block_size_t end_block =
        std::min(start_block + num_blocks_per_thread, num_used_blocks);
    recovery_threads.emplace_back(recover, thread_num, start_block, end_block);
  }

  for (std::thread &thread : recovery_threads) {
    thread.join();
  }

  auto end = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  DEBUG_LOG("RECOVERY DURATION: " << duration << " ms.");
  DEBUG_LOG("Re-inserted " << current_size_.load(LOAD_ORDER) << " keys.");
}

template <>
void Viper<std::string, std::string>::recover_database() {
  // TODO
  throw std::runtime_error("Not implemented yet");
}

template <typename K, typename V>
void Viper<K, V>::get_new_access_information(Client *client) {
  // Get insert/delete count info
  client->info_sync(false);

  // const KVOffset block_page{current_block_page_.load(LOAD_ORDER)};
  // if (block_page.block_number > resize_threshold_ * v_blocks_.size()) {
  //     trigger_resize();
  // }

  get_block_based_access(client);
}

template <typename K, typename V>
void Viper<K, V>::get_new_var_size_access_information(Client *client) {
  if constexpr (!std::is_same_v<K, std::string>) {
    throw std::runtime_error("Cannot update var pages for fixed-size entries.");
  }

  get_new_access_information(client);
  client->v_page_->next_insert_offset = 0;
#ifdef BATCH_COMPACTION
  if (client->is_reclaiming_client_) {
    client->volatile_page_ = new VPage();
    client->volatile_page_->init();
  }
#endif

  v_base_.v_metadata->num_used_blocks.fetch_add(1);
  pmem_persist(v_base_.v_metadata, sizeof(ViperFileMetadata));
}

template <typename K, typename V>
void Viper<K, V>::get_block_based_access(Client *client) {
  if (client->v_block_ != nullptr) {
    client->v_block_->release_ownership();
    // client->v_block_->v_pages[0].version_lock &= NO_CLIENT_BIT;
  }

  block_size_t client_block = -1;
  page_size_t client_page = 0;
  if (client->is_reclaiming_client_) {
    std::lock_guard<std::mutex> lock(reserved_blocks_lock_);
    if (reserved_blocks_.size() > 0) {
      client_block = reserved_blocks_.front();
      reserved_blocks_.pop();
    } else {
      printf("No enough reserved block.\n");
      exit(-1);
    }
  } else {
    bool get_from_queue = free_blocks_.try_dequeue(client_block);
    // if (!free_blocks_.try_dequeue(client_block)) {
    if (!get_from_queue) {
      // No free block available, get new one.
      const KVOffset new_block = get_new_block();
      if (new_block != KVOffset::NONE()) {
        client_block = new_block.block_number;
        client_page = new_block.page_number;
      } else {
        // no free block available
        // printf("wait for free block\n");
        while (!free_blocks_.try_dequeue(client_block)) {
          client->info_sync(true);
        }
      }
    }
  }
  assert(client_block != -1);
  client->used_blocks++;

  client->strategy_ = Client::PageStrategy::BlockBased;
  client->v_block_number_ = client_block;
  client->v_page_number_ = client_page;
  client->num_v_pages_processed_ = 0;

  while (is_v_blocks_resizing_.load(std::memory_order_acquire)) {
    // Wait for vector's memmove to complete. Otherwise we might encounter a
    // segfault.
    asm("nop");
  }

  // if (client->v_block_ != nullptr) {
  //     client->v_block_->release_ownership();
  //     // client->v_block_->v_pages[0].version_lock &= NO_CLIENT_BIT;
  // }

  client->v_block_ = v_blocks_[client_block];
  client->v_page_ = &(client->v_block_->v_pages[client_page]);
  bool res = client->v_block_->get_ownership();
  assert(res);
  client->v_page_->init();
#ifdef REDUCE_PM_ACCESS
  block_gcinfo_[client_block].Clear(client_page);
#endif
  // client->v_block_->v_pages[0].version_lock |= CLIENT_BIT;

  v_base_.v_metadata->num_used_blocks.fetch_add(1, std::memory_order_relaxed);
  pmem_persist(v_base_.v_metadata, sizeof(ViperFileMetadata));
}

template <typename K, typename V>
KeyValueOffset Viper<K, V>::get_new_block() {
  offset_size_t raw_block_page = current_block_page_.load(LOAD_ORDER);
  KVOffset new_offset{};
  block_size_t client_block;
  do {
    const KVOffset v_block_page{raw_block_page};
    if (v_block_page.block_number >=
        v_blocks_.size() - v_config_.num_reclaim_threads) {
      return KVOffset::NONE();
    }
    client_block = v_block_page.block_number;

    const block_size_t new_block = client_block + 1;
    while (client_block >= num_v_blocks_.load(LOAD_ORDER)) {
      asm("nop");
    }
    assert(new_block < v_blocks_.size());

    // Choose random offset to evenly distribute load on all DIMMs
    page_size_t new_page = 0;
    if constexpr (!std::is_same_v<std::string, K>) {
      new_page = rand() % num_pages_per_block;
    }
    new_offset = KVOffset{new_block, new_page, 0};
  } while (!current_block_page_.compare_exchange_weak(raw_block_page,
                                                      new_offset.offset));

  return KVOffset{raw_block_page};
}

template <typename K, typename V>
void Viper<K, V>::trigger_resize() {
  bool expected_resizing = false;
  const bool should_resize =
      is_resizing_.compare_exchange_strong(expected_resizing, true);
  if (!should_resize) {
    return;
  }

  // Only one thread can ever get here because for all others the atomic
  // exchange above fails.
  resize_thread_ = std::make_unique<std::thread>([this] {
    DEBUG_LOG("Start resizing.");
    ViperFileMapping mapping = allocate_v_page_blocks();
    add_v_page_blocks(mapping);
    is_resizing_.store(false, STORE_ORDER);
    DEBUG_LOG("End resizing.");
  });
  resize_thread_->detach();
}

template <typename K, typename V>
void Viper<K, V>::trigger_reclaim(size_t num_reclaim_ops) {
  bool expected_reclaiming = false;
  const bool should_reclaim =
      is_reclaiming_.compare_exchange_strong(expected_reclaiming, true);
  if (!should_reclaim) {
    return;
  }
  DEBUG_LOG("num_reclaims " << num_reclaim_ops);
  reclaimable_ops_.fetch_sub(num_reclaim_ops);

  DEBUG_LOG("BEGIN RECLAIMING");
  ++reclaim_epoch_;

  // if (reclaim_thread_) {
  //     reclaim_thread_->join();
  //     reclaim_thread_.release();
  // }

  // reclaim_thread_ = std::make_unique<std::thread>([this] {
  //     reclaim();
  //     is_reclaiming_.store(false, STORE_ORDER);
  //     // DEBUG_LOG("END RECLAIMING");
  // });
  // reclaim_thread_->detach();
}

template <typename K, typename V>
inline typename Viper<K, V>::Client Viper<K, V>::get_client() {
  // num_active_clients_++;
  int id = num_active_clients_.fetch_add(1);
  Client client{*this};
  client.is_reclaiming_client_ = false;
  client.client_id_ = id;
  if constexpr (std::is_same_v<K, std::string>) {
    get_new_var_size_access_information(&client);
  } else {
    get_new_access_information(&client);
  }
  return client;
}

template <typename K, typename V>
inline typename Viper<K, V>::Client Viper<K, V>::get_reclaim_client() {
  Client client{*this};
  client.is_reclaiming_client_ = true;
  if constexpr (std::is_same_v<K, std::string>) {
    get_new_var_size_access_information(&client);
  } else {
    get_new_access_information(&client);
  }
  return client;
}

template <typename K, typename V>
inline std::unique_ptr<typename Viper<K, V>::ClientWrapper>
Viper<K, V>::get_client_ptr() {
  // num_active_clients_++;
  std::unique_ptr<Viper<K, V>::ClientWrapper> p_client_wrapper =
      std::make_unique<Viper<K, V>::ClientWrapper>(*this);
  return p_client_wrapper;
}

template <typename K, typename V>
typename Viper<K, V>::ReadOnlyClient Viper<K, V>::get_read_only_client() {
  return ReadOnlyClient{*this};
}

template <typename K, typename V>
void Viper<K, V>::remove_client(Viper::Client *client) {
  client->info_sync(true);
  if (!client->is_cold_client_ && !client->is_reclaiming_client_) {
    --num_active_clients_;
  }
}

template <typename K, typename V>
inline bool Viper<K, V>::check_key_equality(const K &key,
                                            const KVOffset offset_to_compare) {
  if constexpr (!using_fp) {
    throw std::runtime_error(
        "Should not use key checker without fingerprints!");
  }

  if (offset_to_compare.is_tombstone()) {
    return false;
  }

  const ReadOnlyClient client = get_read_only_client();
  const auto &entry = client.get_const_entry_from_offset(offset_to_compare);
  if constexpr (std::is_pointer_v<typename KeyAccessor<K>::checker_type>) {
    return *(entry.first) == key;
  } else {
    return entry.first == key;
  }
}

template <typename K, typename V>
bool Viper<K, V>::Client::put(const K &key, const V &value,
                              const bool delete_old,
                              const KVOffset gc_old_offset, Shortcut sc) {
  v_page_->lock();

  // We now have the lock on this page
  std::bitset<VPage::num_slots_per_page> *free_slots = &v_page_->free_slots;
  const data_offset_size_t free_slot_idx = free_slots->_Find_first();

  if (free_slot_idx >= free_slots->size()) {
    // Page is full. Free lock on page and restart.
    v_page_->unlock();
    update_access_information();
    return put(key, value, delete_old);
  }

  // We have found a free slot on this page. Persist data.
  v_page_->data[free_slot_idx] = {key, value};
  typename VPage::VEntry *entry_ptr = v_page_->data.data() + free_slot_idx;
  pmem_persist(entry_ptr, sizeof(typename VPage::VEntry));

  free_slots->reset(free_slot_idx);
  pmem_persist(free_slots, sizeof(*free_slots));

  // Store data in DRAM map.
  const KVOffset kv_offset{v_block_number_, v_page_number_, free_slot_idx};
  KVOffset old_offset;

  if constexpr (using_fp) {
    auto key_check_fn = [&](auto key, auto offset) {
      return this->viper_.check_key_equality(key, offset);
    };
    old_offset = this->viper_.map_.Insert(key, kv_offset, key_check_fn);
  } else {
    old_offset = this->viper_.map_.Insert(key, kv_offset);
  }

  const bool is_new_item = old_offset.is_tombstone();
  if (!is_new_item && delete_old) {
    // Need to free slot at old location for this key
    free_occupied_slot(old_offset, key);
  }

  v_page_->unlock();

  // We have added one value, so +1
  size_delta_++;
  info_sync();

  return is_new_item;
}

template <>
bool Viper<std::string, std::string>::Client::put(const std::string &key,
                                                  const std::string &value,
                                                  const bool delete_old,
                                                  const KVOffset gc_old_offset,
                                                  Shortcut sc) {
RETRY:
  v_page_->lock();
  VPage *start_v_page = v_page_;

  internal::VarSizeEntry entry{key.size(), value.size()};
  char *insert_pos = v_page_->data.data() + v_page_->next_insert_offset;
  const size_t entry_length = entry.key_size + entry.value_size;
  const size_t meta_size = sizeof(entry.size_info);
  const size_t insert_offset_size = sizeof(VPage::next_insert_offset);

  ptrdiff_t offset_in_page = insert_pos - reinterpret_cast<char *>(v_page_);
  assert(offset_in_page <= v_page_size);

  block_size_t current_block_number = v_block_number_;
  page_size_t current_page_number = v_page_number_;
  VPage *current_v_page = v_page_;

  uint64_t tmp_offset = offset_in_page + meta_size + entry_length;
  if (has_shortcut_) {
    tmp_offset += sizeof(Shortcut) * shortcut_buffer_.size();
  }

  if (tmp_offset > v_page_size) {
    // This page is not big enough to fit the record. Allocate new one.
    // if (offset_in_page + meta_size <= v_page_size) {
    if (tmp_offset - entry_length <= v_page_size) {
      // 0-entry metadata fits into current page.
      internal::VarSizeEntry next_page_entry{0, 0};
      pmem_memcpy_persist(insert_pos, &next_page_entry.size_info, meta_size);
      insert_pos += meta_size;
    }
    if (has_shortcut_) {
      v_page_->shortcut_start_offset =
          insert_pos - reinterpret_cast<char *>(v_page_) - VPage::METADATA_SIZE;
      assert(v_page_->shortcut_start_offset +
                 sizeof(Shortcut) * shortcut_buffer_.size() <=
             v_page_size);
      pmem_memcpy_persist(insert_pos, shortcut_buffer_.data(),
                          sizeof(Shortcut) * shortcut_buffer_.size());
      shortcut_buffer_.clear();
    }
    v_page_->has_shortcut = has_shortcut_;
    v_page_->offset = offset_in_page - VPage::METADATA_SIZE;
    v_page_->next_insert_offset = VPage::DATA_SIZE;
    v_page_->unlock();
    pmem_persist(v_page_, VPage::METADATA_SIZE);
    update_var_size_page_information();
    goto RETRY;
  }

  // Entire record fits into current page.
  entry.data = insert_pos + meta_size;
  memcpy(insert_pos, &entry.size_info, meta_size);
  memcpy(entry.data, key.data(), entry.key_size);
  memcpy(entry.data + entry.key_size, value.data(), entry.value_size);
  pmem_persist(insert_pos, meta_size + entry_length);
  v_page_->next_insert_offset += meta_size + entry_length;
  pmem_persist(v_page_, insert_offset_size);
  _mm_prefetch(v_page_, _MM_HINT_T0);

  const uint16_t data_offset = offset_in_page - v_page_->METADATA_SIZE;
  const KVOffset var_offset{current_block_number, current_page_number,
                            data_offset, uint16_t(meta_size + entry_length)};
  bool is_new_item = true;
  KVOffset old_offset = KVOffset::Tombstone();

  // Store data in DRAM map.
  auto key_check_fn = [&](auto key, auto offset) {
    return this->viper_.check_key_equality(key, offset);
  };

  if (is_reclaiming_client_) {
    TIMER_START_LOGGING(update_index_time_);

    if (!sc.None() && this->viper_.map_.TryGCUpdate(
                          key, var_offset, key_check_fn, &old_offset, sc)) {
      COUNTER_ADD_LOGGING(num_shortcut_succeed_, 1);
    } else {
      old_offset = this->viper_.map_.Insert(key, var_offset, key_check_fn);
    }
    TIMER_STOP_LOGGING(update_index_time_);
    COUNTER_ADD_LOGGING(num_total_put_, 1);
  } else {
#ifdef GC_SHORTCUT
    Shortcut this_sc;
    old_offset =
        this->viper_.map_.Insert(key, var_offset, key_check_fn, &this_sc);
    if (has_shortcut_) {
      shortcut_buffer_.push_back(this_sc);
    }
#else
    old_offset = this->viper_.map_.Insert(key, var_offset, key_check_fn);
#endif
  }
  is_new_item = old_offset.is_tombstone();
  size_delta_++;

  start_v_page->unlock();

  // Need to free slot at old location for this key
  if (!is_new_item && delete_old) {
#ifdef REDUCE_PM_ACCESS
    const auto [block_number, page_number, data_offset] = old_offset.get_offsets();
    this->viper_.block_gcinfo_[old_offset.block_number].SetGarbage(
        page_number, data_offset);
    __atomic_fetch_add(
        &viper_.block_gcinfo_[block_number].modified_bytes[page_number],
        old_offset.object_size, __ATOMIC_ACQ_REL);
    num_reclaimable_ops_++;
    --size_delta_;
#else
    free_occupied_slot(old_offset, key);
#endif
  }
  if (!gc_old_offset.is_tombstone()) {
    assert(!is_new_item && is_reclaiming_client_);
    if (old_offset != gc_old_offset) {
      // cleaner move a stale object (wrong), but set other as garbage
#ifdef REDUCE_PM_ACCESS
      const auto [block_number, page_number, data_offset] =
          old_offset.get_offsets();
      this->viper_.block_gcinfo_[old_offset.block_number].SetGarbage(
          page_number, data_offset);
      __atomic_fetch_add(
          &viper_.block_gcinfo_[block_number].modified_bytes[page_number],
          old_offset.object_size, __ATOMIC_ACQ_REL);
      num_reclaimable_ops_++;
      --size_delta_;
#else
      free_occupied_slot(old_offset, key);
#endif
    } else {
      // free_occupied_slot(gc_old_offset, key);  // no lock
      // const auto [block_number, page_number, data_offset] =
      //     gc_old_offset.get_offsets();
      // this->viper_.block_gcinfo_[block_number].SetGarbage(page_number,
      //                                                     data_offset);
    }
  }

  info_sync();
  return is_new_item;
}

#ifdef BATCH_COMPACTION

template <typename K, typename V>
void Viper<K, V>::Client::FlushAndInsertOffsets() {
  throw std::runtime_error("not supported.");
}

template <>
void Viper<std::string, std::string>::Client::FlushAndInsertOffsets() {
  data_offset_size_t diff =
      volatile_page_->next_insert_offset - v_page_->next_insert_offset;
  if (diff == 0) {
    assert(to_insert_offset_.size() == 0);
    // nothing to do
    return;
  }

  pmem_memcpy_persist(v_page_->data.data() + v_page_->next_insert_offset,
                      volatile_page_->data.data() + v_page_->next_insert_offset,
                      diff);
  v_page_->next_insert_offset = volatile_page_->next_insert_offset;
  v_page_->offset = volatile_page_->offset;
  v_page_->has_shortcut = false;
  pmem_persist(v_page_, VPage::METADATA_SIZE);

  for (int i = 0; i < to_insert_offset_.size(); i++) {
    KVOffset old_offset = KVOffset::Tombstone();
    KVOffset var_offset = to_insert_offset_[i].new_offset;
    auto key_check_fn = [&](auto key, auto offset) {
      return this->viper_.check_key_equality(key, offset);
    };
    Shortcut sc = to_insert_offset_[i].sc;
    if (!sc.None() &&
        this->viper_.map_.TryGCUpdate(to_insert_offset_[i].key, var_offset,
                                      key_check_fn, &old_offset, sc)) {
      COUNTER_ADD_LOGGING(num_shortcut_succeed_, 1);
    } else {
      old_offset = this->viper_.map_.Insert(to_insert_offset_[i].key,
                                            var_offset, key_check_fn);
    }
    COUNTER_ADD_LOGGING(num_total_put_, 1);
    size_delta_++;
    if (unlikely(old_offset.is_tombstone())) {
      // throw new std::runtime_error("the old is tombstone");
      DEBUG_LOG("CCEH bug: the old is tombstone");
    } else if (old_offset != to_insert_offset_[i].old_offset) {
      // cleaner move a stale object (wrong), but set other as garbage
#ifdef REDUCE_PM_ACCESS
      const auto [block_number, page_number, data_offset] =
          old_offset.get_offsets();
      this->viper_.block_gcinfo_[old_offset.block_number].SetGarbage(
          page_number, data_offset);
      __atomic_fetch_add(
          &viper_.block_gcinfo_[block_number].modified_bytes[page_number], 64,
          __ATOMIC_ACQ_REL);
      num_reclaimable_ops_++;
      --size_delta_;
#else
      free_occupied_slot(old_offset, to_insert_offset_[i].key);
#endif
      info_sync();
    }
  }
  to_insert_offset_.clear();
}

template <typename K, typename V>
void Viper<K, V>::Client::ReclaimBatchPut(const K &key, const V &value,
                                          const KVOffset old_offset,
                                          Shortcut sc) {
  throw std::runtime_error("not supported.");
}

template <>
void Viper<std::string, std::string>::Client::ReclaimBatchPut(
    const std::string &key, const std::string &value,
    const KVOffset old_offset, Shortcut sc) {
RETRY:
  internal::VarSizeEntry entry{key.size(), value.size()};
  bool is_inserted = false;
  char *insert_pos =
      volatile_page_->data.data() + volatile_page_->next_insert_offset;
  const size_t entry_length = entry.key_size + entry.value_size;
  const size_t meta_size = sizeof(entry.size_info);

  ptrdiff_t offset_in_page =
      insert_pos - reinterpret_cast<char *>(volatile_page_);
  assert(offset_in_page <= v_page_size);

  if (offset_in_page + meta_size + entry_length > v_page_size) {
    if (offset_in_page + meta_size <= v_page_size) {
      // 0-entry metadata fits into current page
      internal::VarSizeEntry next_page_entry{0, 0};
      memcpy(insert_pos, &next_page_entry.size_info, meta_size);
    }
    // flush page and insert KVOffsets
    volatile_page_->offset = offset_in_page - VPage::METADATA_SIZE;
    volatile_page_->next_insert_offset = VPage::DATA_SIZE;
    FlushAndInsertOffsets();
    update_var_size_page_information();
    // clean volatile page
    goto RETRY;
  }

  // Entire record fits into current page.
  entry.data = insert_pos + meta_size;
  memcpy(insert_pos, &entry.size_info, meta_size);
  memcpy(entry.data, key.data(), entry.key_size);
  memcpy(entry.data + entry.key_size, value.data(), entry.value_size);
  volatile_page_->next_insert_offset += meta_size + entry_length;

  const uint16_t data_offset = offset_in_page - volatile_page_->METADATA_SIZE;
  const KVOffset var_offset{v_block_number_, v_page_number_, data_offset,
                            uint16_t(meta_size + entry_length)};
  to_insert_offset_.emplace_back(key, old_offset, var_offset, sc);
  assert(volatile_page_->next_insert_offset - v_page_->next_insert_offset == to_insert_offset_.size() * uint16_t(meta_size + entry_length));
}

#endif

/**
 * Insert a `value` for a given `key`.
 * Returns true if the item in new, i.e., the key was not present in Viper,
 * or false if it replaces an existing value.
 */
template <typename K, typename V>
bool Viper<K, V>::Client::put(const K &key, const V &value) {
  return put(key, value, true);
}

/**
 * Get the `value` for a given `key`.
 * Returns true if the item was found or false if not.
 * If the item was found, `value` will contain the found entry.
 * If it was not found, `value` is not modified and will contain whatever was
 * present before the call.
 */
template <typename K, typename V>
bool Viper<K, V>::Client::get(const K &key, V *value) {
  auto key_check_fn = [&](auto key, auto offset) {
    if constexpr (using_fp) {
      return this->viper_.check_key_equality(key, offset);
    } else {
      return cceh::CCEH<K>::dummy_key_check(key, offset);
    }
  };

  while (true) {
    KVOffset kv_offset = this->viper_.map_.Get(key, key_check_fn);
    if (kv_offset.is_tombstone()) {
      return false;
    }
    if (get_value_from_offset(kv_offset, value)) {
      return true;
    }
  }
}

/**
 * Get the `value` for a given `key`.
 * Returns true if the item was found or false if not.
 * If the item was found, `value` will contain the found entry.
 * If it was not found, `value` is not modified and will contain whatever was
 * present before the call.
 */
template <typename K, typename V>
bool Viper<K, V>::ReadOnlyClient::get(const K &key, V *value) const {
  auto key_check_fn = [&](auto key, auto offset) {
    if constexpr (using_fp) {
      return this->viper_.check_key_equality(key, offset);
    } else {
      return cceh::CCEH<K>::dummy_key_check(key, offset);
    }
  };

  while (true) {
    KVOffset kv_offset = this->viper_.map_.Get(key, key_check_fn);
    if (kv_offset.is_tombstone()) {
      return false;
    }
    if (get_const_value_from_offset(kv_offset, value)) {
      return true;
    }
  }
}

/**
 * Get the `value` for a given `key`.
 * Returns true if the item was found or false if not.
 * If the item was found, `value` will contain the found entry.
 * If it was not found, `value` is not modified and will contain whatever was
 * present before the call.
 */
template <typename K, typename V>
bool Viper<K, V>::Client::get(const K &key, V *value) const {
  return static_cast<const Viper<K, V>::ReadOnlyClient *>(this)->get(key,
                                                                     value);
}

/**
 * Updates the value for a given `key` atomically.
 * For non-atomic updates, use `put()`.
 * Returns true if the item was updated successfully, false otherwise.
 * `update_fn` should be a function that atomically updated a value and handles
 * persistence, e.g., through a call to `pmem_persist`. If the modification is
 * not atomic, Viper cannot guarantee correctness.
 */
template <typename K, typename V>
template <typename UpdateFn>
bool Viper<K, V>::Client::update(const K &key, UpdateFn update_fn) {
  if constexpr (std::is_same_v<K, std::string>) {
    throw std::runtime_error(
        "In-place update not supported for variable length records!");
  }

  auto key_check_fn = [&](auto key, auto offset) {
    if constexpr (using_fp) {
      return this->viper_.check_key_equality(key, offset);
    } else {
      return cceh::CCEH<K>::dummy_key_check(key, offset);
    }
  };

  while (true) {
    const KVOffset kv_offset = this->viper_.map_.Get(key, key_check_fn);
    if (kv_offset.is_tombstone()) {
      return false;
    }

    const auto [block, page, slot] = kv_offset.get_offsets();
    VPage &v_page = this->viper_.v_blocks_[block]->v_pages[page];
    if (!v_page.lock(false)) {
      // Could not lock page, so the record could be modified and we need to try
      // again
      continue;
    }

    update_fn(&(v_page.data[slot].second));
    v_page.unlock();
    return true;
  }
}

/**
 * Delete the value for a given `key`.
 * Returns true if the item was deleted or false if not.
 */
template <typename K, typename V>
bool Viper<K, V>::Client::remove(const K &key) {
  auto key_check_fn = [&](auto key, auto offset) {
    if constexpr (using_fp) {
      return this->viper_.check_key_equality(key, offset);
    } else {
      return cceh::CCEH<K>::dummy_key_check(key, offset);
    }
  };

  const KVOffset kv_offset = this->viper_.map_.Get(key, key_check_fn);
  if (kv_offset.is_tombstone()) {
    return false;
  }

  free_occupied_slot(kv_offset, key, true);
  // num_reclaimable_ops_++;  // move to free_occupied_slot
  return true;
}

template <typename K, typename V>
void Viper<K, V>::Client::free_occupied_slot(const KVOffset offset_to_delete,
                                             const K &key,
                                             const bool delete_offset) {
  num_reclaimable_ops_++;
  const auto [block_number, page_number, data_offset] =
      offset_to_delete.get_offsets();
  while (this->viper_.is_v_blocks_resizing_.load(LOAD_ORDER)) {
    // Wait for vector's memmove to complete. Otherwise we might encounter a
    // segfault.
  }

  auto key_check_fn = [&](auto key, auto offset) {
    if constexpr (using_fp) {
      return this->viper_.check_key_equality(key, offset);
    } else {
      return cceh::CCEH<K>::dummy_key_check(key, offset);
    }
  };

  if (v_block_number_ == block_number && v_page_number_ == page_number) {
    // Old record to delete is on the same page. We already hold the lock here.
    invalidate_record(v_page_, offset_to_delete);
    if (delete_offset) {
      this->viper_.map_.Insert(key, IndexV::NONE(), key_check_fn);
    }
    --size_delta_;
    return;
  }

  VPage &v_page = this->viper_.v_blocks_[block_number]->v_pages[page_number];
  std::atomic<version_lock_t> &v_lock = v_page.version_lock;
  version_lock_t lock_value = v_lock.load(LOAD_ORDER);
  lock_value &= UNLOCKED_BIT;

  const size_t num_deadlock_retries = 32;
  std::vector<KVOffset> &deadlock_offsets = this->viper_.deadlock_offsets_;
  bool deadlock_offset_inserted = false;
  bool encountered_own_offset = false;
  bool has_lock;

  // Acquire lock or clear deadlock loop
  while (true) {
    size_t retries = 0;
    has_lock = true;
    while (!v_lock.compare_exchange_weak(lock_value, lock_value + 1)) {
      // Client may be in a deadlock situation
      lock_value &= UNLOCKED_BIT;
      if (++retries == num_deadlock_retries) {
        has_lock = false;
        break;
      }
      _mm_pause();
    }

    if (has_lock) {
      // Acquired lock, delete normally
      bool has_deleted = false;
      if (deadlock_offset_inserted) {
        // remove from deadlock_offsets
        bool expected_offset_lock = false;
        while (!this->viper_.deadlock_offset_lock_.compare_exchange_weak(
            expected_offset_lock, true)) {
          expected_offset_lock = false;
          _mm_pause();
        }
        auto own_item_pos = std::find(deadlock_offsets.begin(),
                                      deadlock_offsets.end(), offset_to_delete);
        if (own_item_pos != deadlock_offsets.end()) {
          deadlock_offsets.erase(own_item_pos);
        } else {
          has_deleted = true; // removed by another thread
        }
        deadlock_offset_inserted = false;
        this->viper_.deadlock_offset_lock_.store(false);
      }
      if (!has_deleted) {
        invalidate_record(&v_page, offset_to_delete);
      }
      break;
    }

    // Check possible deadlock
    bool expected_offset_lock = false;
    while (!this->viper_.deadlock_offset_lock_.compare_exchange_weak(
        expected_offset_lock, true)) {
      expected_offset_lock = false;
      _mm_pause();
    }

    if (!deadlock_offset_inserted) {
      deadlock_offsets.push_back(offset_to_delete);
      deadlock_offset_inserted = true;
    }

    std::vector<KVOffset> new_offsets;
    new_offsets.reserve(deadlock_offsets.size());
    encountered_own_offset = false;
    for (KVOffset offset : deadlock_offsets) {
      encountered_own_offset |= offset_to_delete == offset;
      if (offset.block_number != v_block_number_ ||
          offset.page_number != v_page_number_) {
        new_offsets.push_back(offset);
      } else {
        invalidate_record(v_page_, offset);
      }
    }
    deadlock_offsets = std::move(new_offsets);
    this->viper_.deadlock_offset_lock_.store(false);

    if (!encountered_own_offset) {
      // The old offset that this client need to delete was deleted by another
      // client, we can return.
      break;
    }
  }

  if (delete_offset) {
    this->viper_.map_.Insert(key, IndexV::NONE(), key_check_fn);
  }

  if (has_lock) {
    if (deadlock_offset_inserted && encountered_own_offset) {
      // We inserted into the queue but manually deleted.
      bool expected_offset_lock = false;
      while (!this->viper_.deadlock_offset_lock_.compare_exchange_weak(
          expected_offset_lock, true)) {
        expected_offset_lock = false;
        _mm_pause();
      }

      auto own_item_pos = std::find(deadlock_offsets.begin(),
                                    deadlock_offsets.end(), offset_to_delete);
      if (own_item_pos != deadlock_offsets.end()) {
        deadlock_offsets.erase(own_item_pos);
      }
      this->viper_.deadlock_offset_lock_.store(false);
    }
    v_page.unlock();
  }

  --size_delta_;
}

template <typename K, typename V>
inline void Viper<K, V>::Client::invalidate_record(
    VPage *v_page, const KVOffset kv_offset) {
  const auto [block_number, page_number, data_offset] = kv_offset.get_offsets();
  if constexpr (std::is_same_v<K, std::string>) {
#ifdef REDUCE_PM_ACCESS
    this->viper_.block_gcinfo_[block_number].SetGarbage(page_number,
                                                        data_offset);
    const size_t entry_size = kv_offset.object_size;
    this->viper_.block_gcinfo_[block_number].modified_bytes[page_number] +=
        entry_size;
#else
    char *raw_data = &v_page->data[data_offset];
    internal::VarSizeEntry *var_entry =
        reinterpret_cast<internal::VarSizeEntry *>(raw_data);
    var_entry->is_set = false;
    const size_t meta_size = sizeof(var_entry->size_info);
    pmem_persist(&var_entry->size_info, meta_size);
    const size_t entry_size =
        var_entry->key_size + var_entry->value_size + meta_size;
    v_page->modified_bytes += entry_size;
#endif
  } else {
    auto *free_slots = &v_page->free_slots;
    free_slots->set(data_offset);
    pmem_persist(free_slots, sizeof(*free_slots));
  }
}

template <typename K, typename V>
void Viper<K, V>::Client::update_access_information() {
  if (strategy_ == PageStrategy::DimmBased) {
    if (v_block_number_ == end_v_block_number_) {
      // No more allocated pages, need new range
      this->viper_.get_new_access_information(this);
    } else {
      ++v_block_number_;
      v_block_ = this->viper_.v_blocks_[v_block_number_];
      v_page_ = &(v_block_->v_pages[v_page_number_]);
    }
  } else if (strategy_ == PageStrategy::BlockBased) {
    if (++num_v_pages_processed_ == this->viper_.num_pages_per_block) {
      // No more pages, need new block
      this->viper_.get_new_access_information(this);
    } else {
      v_page_number_ = (v_page_number_ + 1) % this->viper_.num_pages_per_block;
      v_page_ = &(v_block_->v_pages[v_page_number_]);
    }
  } else {
    throw std::runtime_error("Unknown page strategy.");
  }

  // Make sure new page is initialized correctly
  v_page_->init();
#ifdef REDUCE_PM_ACCESS
  this->viper_.block_gcinfo_[v_block_number_].Clear(v_page_number_);
#endif
}

template <typename K, typename V>
void Viper<K, V>::Client::update_var_size_page_information() {
  update_access_information();
  v_page_->next_insert_offset = 0;
#ifdef BATCH_COMPACTION
  if (is_reclaiming_client_) {
    volatile_page_->init();
  }
#endif
}

template <typename K, typename V>
void Viper<K, V>::Client::info_sync(const bool force) {
  if (force || ++op_count_ == 10000) {
    this->viper_.current_size_.fetch_add(size_delta_);

    if (this->viper_.v_config_.enable_reclamation) {
      size_t num_reclaims =
          this->viper_.reclaimable_ops_.fetch_add(num_reclaimable_ops_);
      num_reclaims += num_reclaimable_ops_;
      if (force || num_reclaims > this->viper_.reclaim_threshold_) {
        this->viper_.trigger_reclaim(num_reclaims);
      }
    }

    op_count_ = 0;
    size_delta_ = 0;
    num_reclaimable_ops_ = 0;
  }
}

template <typename K, typename V>
Viper<K, V>::ReadOnlyClient::ReadOnlyClient(ViperT &viper) : viper_{viper} {}

template <typename K, typename V>
Viper<K, V>::Client::Client(ViperT &viper) : ReadOnlyClient{viper} {
  op_count_ = 0;
  size_delta_ = 0;
  num_reclaimable_ops_ = 0;
  num_v_pages_processed_ = 0;
  v_block_number_ = 0;
  v_page_number_ = 0;
  end_v_block_number_ = 0;
  v_page_ = nullptr;
  v_block_ = nullptr;
}

template <typename K, typename V>
Viper<K, V>::Client::~Client() {
  this->viper_.remove_client(this);
  if (v_block_ != nullptr) {
    // v_block_->v_pages[0].version_lock &= NO_CLIENT_BIT;
    v_block_->release_ownership();
  }
  if (is_reclaiming_client_) {
#ifdef BATCH_COMPACTION
    if (volatile_page_) {
      delete volatile_page_;
    }
#endif
    LOG("cleaner %d: total clean time %lu update index time %lu shortcut %lu "
        "all %lu ratio %lf",
        client_id_,
        this->viper_.local_reclaim_time_ns_
                [client_id_ - this->viper_.v_config_.num_client_threads] /
            1000,
        update_index_time_ / 1000, num_shortcut_succeed_, num_total_put_,
        (double)num_shortcut_succeed_ / num_total_put_);
  }
}

template <typename K, typename V>
inline const std::pair<typename KeyAccessor<K>::checker_type,
                       typename ValueAccessor<V>::checker_type>
Viper<K, V>::ReadOnlyClient::get_const_entry_from_offset(
    Viper::KVOffset offset) const {
  if constexpr (std::is_same_v<K, std::string>) {
    const auto [block, page, data_offset] = offset.get_offsets();
    const VPageBlock *v_block = this->viper_.v_blocks_[block];
    const VPage &v_page = v_block->v_pages[page];
    const char *raw_data = &v_page.data[data_offset];
    internal::VarEntryAccessor var_entry{raw_data};
    if (var_entry.value_size == 0) {
      // Value is on next page
      const char *raw_value_data = &v_block->v_pages[page + 1].data[0];
      var_entry = internal::VarEntryAccessor{raw_data, raw_value_data};
    }
    return {var_entry.key(), var_entry.value()};
  } else {
    const auto [block, page, slot] = offset.get_offsets();
    const auto &entry = this->viper_.v_blocks_[block]->v_pages[page].data[slot];
    return {&entry.first, &entry.second};
  }
}

template <typename K, typename V>
inline bool Viper<K, V>::ReadOnlyClient::get_const_value_from_offset(
    KVOffset offset, V *value) const {
  const auto [block, page, slot] = offset.get_offsets();
  const VPage &v_page = this->viper_.v_blocks_[block]->v_pages[page];
  const std::atomic<version_lock_t> &page_lock = v_page.version_lock;
  version_lock_t lock_val = page_lock.load(LOAD_ORDER);
  if (IS_LOCKED(lock_val)) {
    return false;
  }
  const auto entry = this->get_const_entry_from_offset(offset);
  if constexpr (std::is_same_v<K, std::string>) {
    const std::string_view &str_val = entry.second;
    value->assign(str_val.data(), str_val.size());
  } else {
    *value = *(entry.second);
  }
  return lock_val == page_lock.load(LOAD_ORDER);
}

/** Return the total number of used bytes in PMem */
template <typename K, typename V>
size_t Viper<K, V>::ReadOnlyClient::get_total_used_pmem() const {
  // + PAGE_SIZE for metadata block
  return (this->viper_.v_base_.v_metadata->num_used_blocks *
          sizeof(VPageBlock)) +
         PAGE_SIZE;
}

/** Return the total number of allocated bytes in PMem */
template <typename K, typename V>
size_t Viper<K, V>::ReadOnlyClient::get_total_allocated_pmem() const {
  return this->viper_.v_base_.v_metadata->total_mapped_size;
}

template <typename K, typename V>
inline bool Viper<K, V>::Client::get_value_from_offset(const KVOffset offset,
                                                       V *value) {
  const auto [block, page, slot] = offset.get_offsets();
  const VPage &v_page = this->viper_.v_blocks_[block]->v_pages[page];
  const std::atomic<version_lock_t> &page_lock = v_page.version_lock;
  version_lock_t lock_val = page_lock.load(LOAD_ORDER);
  if (IS_LOCKED(lock_val)) {
    return false;
  }
  *value = v_page.data[slot].second;
  return lock_val == page_lock.load(LOAD_ORDER);
}

template <>
inline bool Viper<std::string, std::string>::Client::get_value_from_offset(
    const KVOffset offset, std::string *value) {
  const auto [block, page, data_offset] = offset.get_offsets();
  const VPageBlock *v_block = this->viper_.v_blocks_[block];
  const VPage &v_page = v_block->v_pages[page];
  const std::atomic<version_lock_t> &page_lock = v_page.version_lock;
  version_lock_t lock_val = page_lock.load(LOAD_ORDER);
  if (IS_LOCKED(lock_val)) {
    return false;
  }

  const char *raw_data = &v_page.data[data_offset];
  internal::VarEntryAccessor var_entry{raw_data};
  if (var_entry.value_size == 0) {
    // Value is on next page
    const char *raw_value_data = &v_block->v_pages[page + 1].data[0];
    var_entry = internal::VarEntryAccessor{raw_data, raw_value_data};
  }
  value->assign(var_entry.value_data, var_entry.value_size);
  return lock_val == page_lock.load(LOAD_ORDER);
}


// ClientWrapper
template <typename K, typename V>
Viper<K, V>::ClientWrapper::ClientWrapper(ViperT &viper) : viper_(viper) {
  int id = this->viper_.num_active_clients_.fetch_add(1);
  client_wrapper_id_ = id;
  client_ = new Client(viper);
  client_->client_id_ = id;
#if defined(GC_SHORTCUT) && !defined(HOT_COLD_SEPARATE)
  client_->has_shortcut_ = true;
#endif
  if constexpr (std::is_same_v<K, std::string>) {
    this->viper_.get_new_var_size_access_information(client_);
  } else {
    this->viper_.get_new_access_information(client_);
  }
#ifdef HOT_COLD_SEPARATE
  cold_client_ = new Client(viper);
  cold_client_->client_id_ = id;
  cold_client_->is_cold_client_ = true;
#ifdef GC_SHORTCUT
  cold_client_->has_shortcut_ = true;
#endif
  if constexpr (std::is_same_v<K, std::string>) {
    this->viper_.get_new_var_size_access_information(cold_client_);
  } else {
    this->viper_.get_new_access_information(cold_client_);
  }
#endif
}

template <typename K, typename V>
Viper<K, V>::ClientWrapper::~ClientWrapper() {
  delete client_;
#ifdef HOT_COLD_SEPARATE
  delete cold_client_;
#endif
}

template <typename K, typename V>
bool Viper<K, V>::ClientWrapper::put(const K &key, const V &value) {
#ifdef HOT_COLD_SEPARATE
  bool hot = this->viper_.hot_key_set_->Exist(key);
  bool is_new;
  if (hot) {
    is_new = client_->put(key, value);
  } else {
    is_new = cold_client_->put(key, value);
  }
  if (!is_new) {
    this->viper_.hot_key_set_->Record(key, client_wrapper_id_, hot);
  }
  return is_new;
#else
  return client_->put(key, value);
#endif
}

template <typename K, typename V>
bool Viper<K, V>::ClientWrapper::get(const K &key, V *value) const {
  return client_->get(key, value);
}

template <typename K, typename V>
template <typename UpdateFn>
bool Viper<K, V>::ClientWrapper::update(const K &key, UpdateFn update_fn) {
  return client_->update(key, update_fn);
}

template <typename K, typename V>
bool Viper<K, V>::ClientWrapper::remove(const K &key) {
  return client_->remove(key);
}


template <typename K, typename V>
void Viper<K, V>::compact(Client &client, VPageBlock *v_block,
                          block_size_t block_number) {
  for (VPage &v_page : v_block->v_pages) {
    v_page.lock();
    auto &free_slots = v_page.free_slots;
    for (size_t slot = 0; slot < v_page.num_slots_per_page; ++slot) {
      if (free_slots[slot]) {
        // Slot is free
        continue;
      }

      const auto &record = v_page.data[slot];
      client.put(record.first, record.second, false);
      free_slots[slot] = 1;
      pmem_persist(&v_page.free_slots, sizeof(v_page.free_slots));
    }
    v_page.clean_used_bit();
    v_page.unlock();
  }
}

template <>
void Viper<std::string, std::string>::compact(Client &client,
                                              VPageBlock *v_block,
                                              block_size_t block_number) {
  const size_t meta_size = sizeof(internal::VarSizeEntry::size_info);

  page_size_t current_page = 0;
  VPage *v_page = &v_block->v_pages[current_page];
  v_page->lock();
  const char *raw_data = v_page->data.data();
  uint16_t next_insert_off = v_page->next_insert_offset;
  bool is_last_page = next_insert_off != VPage::DATA_SIZE;

  auto key_check_fn = [&](auto key, auto offset) {
    return check_key_equality(key, offset);
  };

  Shortcut *shortcuts = nullptr;
  bool has_sc = v_page->has_shortcut;
  if (has_sc) {
    shortcuts =
        (Shortcut *)(v_page->data.data() + v_page->shortcut_start_offset);
  }

  while (true) {
    internal::VarEntryAccessor var_entry{raw_data};
    Shortcut sc;
    if (has_sc) {
      sc = *shortcuts;
      shortcuts++;
    }
    data_offset_size_t data_skip =
        meta_size + var_entry.key_size + var_entry.value_size;
    const ptrdiff_t offset_in_page =
        raw_data - reinterpret_cast<char *>(v_page);
    const bool end_of_page = (offset_in_page + meta_size) > v_page_size;

    if (is_last_page && raw_data == v_page->data.data() + next_insert_off) {
      // Previous client has not written further than this, so we can stop here.
      break;
    }

    // No more entries in this page
    if (end_of_page || (var_entry.key_size == 0 && var_entry.value_size == 0)) {
      if (current_page + 1 == num_pages_per_block) {
        break;
      }

      v_page->clean_used_bit();
      v_page->unlock();
      v_page = &v_block->v_pages[++current_page];
      v_page->lock();
      has_sc = v_page->has_shortcut;
      if (has_sc) {
        shortcuts =
            (Shortcut *)(v_page->data.data() + v_page->shortcut_start_offset);
      }
      next_insert_off = v_page->next_insert_offset;
      raw_data = v_page->data.data();
      is_last_page = next_insert_off != VPage::DATA_SIZE;
      continue;
    }

    // Value is on next page
    if (var_entry.value_size == 0) {
      ERROR_EXIT("could not happen");
      // Value is on next page
      if (current_page + 1 == num_pages_per_block) {
        break;
      }

      v_page->clean_used_bit();
      v_page->unlock();
      v_page = &v_block->v_pages[++current_page];
      v_page->lock();
      next_insert_off = v_page->next_insert_offset;
      is_last_page = next_insert_off != VPage::DATA_SIZE;
      const char *raw_value_data = v_page->data.data();
      var_entry = internal::VarEntryAccessor{raw_data, raw_value_data};
      raw_data = raw_value_data + meta_size + var_entry.value_size;
      data_skip = 0;
    }

    // Insert record into new block
    data_offset_size_t data_offset = offset_in_page - VPage::METADATA_SIZE;
#ifdef REDUCE_PM_ACCESS
    bool is_valid =
        !block_gcinfo_[block_number].IsGarbage(current_page, data_offset);
#else
    bool is_valid = var_entry.is_set;
#endif
    if (is_valid) {
      const std::string key{var_entry.key()};
      IndexV old_offset = {block_number, current_page, data_offset, data_skip};
#ifdef BATCH_COMPACTION
      client.ReclaimBatchPut(key, std::string{var_entry.value()}, old_offset, sc);
#else
      client.put(key, std::string{var_entry.value()}, false, old_offset, sc);
      var_entry.is_set = false;
      pmem_persist(&var_entry.is_set, sizeof(var_entry.is_set));
#endif
    }
    raw_data += data_skip;
  }

  v_page->clean_used_bit();
  v_page->unlock();

#ifdef BATCH_COMPACTION
  client.FlushAndInsertOffsets();
#endif
}

template <typename K, typename V>
void Viper<K, V>::reclaim_work(int id, Client &client) {
  const size_t num_slots_per_block =
      num_pages_per_block * VPage::num_slots_per_page;
  const block_size_t max_block =
      KVOffset{current_block_page_.load(LOAD_ORDER)}.block_number;

  // At least X percent of the block should be free before reclaiming it.
  const size_t free_threshold = reclaim_percentage_ * num_slots_per_block;
  block_size_t local_freed_blocks = 0;

  const int step = num_reclaim_threads_;
  for (block_size_t block_num = id; block_num < max_block; block_num += step) {
    Timer timer(local_reclaim_time_ns_[id]);
    VPageBlock *v_block = v_blocks_[block_num];
    size_t block_free_slots = 0;
    if (v_block->is_unused()) {
      // Block in use by client or already marked as free.
      continue;
    }
    if (!v_block->get_ownership()) {
      continue;
    }
    assert(v_block->is_owned());

    for (const VPage &v_page : v_block->v_pages) {
      block_free_slots += v_page.free_slots.count();
    }

    if (block_free_slots > free_threshold) {
      compact(client, v_block, block_num);
      // VPage& head_page = v_block->v_pages[0];
      // head_page.version_lock = 0;
      v_block->clean_version_lock();
      {
        std::lock_guard<std::mutex> lock(reserved_blocks_lock_);
        if (reserved_blocks_.size() < num_reclaim_threads_) {
          reserved_blocks_.push(block_num);
        } else {
          free_blocks_.enqueue(block_num);
        }
      }
      ++local_freed_blocks;
    } else {
      v_block->release_ownership();
    }
  }
  local_reclaim_blocks_[id] += local_freed_blocks;
  reclaimed_blocks_ += local_freed_blocks;

  DEBUG_LOG("LOCAL FREED BLOCKS: " << local_freed_blocks);
}

template <>
void Viper<std::string, std::string>::reclaim_work(int id, Client &client) {
  // const uint32_t modified_threshold =
  //     reclaim_percentage_ * num_pages_per_block * VPage::DATA_SIZE;
  block_size_t local_freed_blocks = 0;

  const block_size_t max_block =
      KVOffset{current_block_page_.load(LOAD_ORDER)}.block_number;

  const int step = num_reclaim_threads_;

  for (block_size_t block_num = id; block_num < max_block; block_num += step) {
    Timer timer(local_reclaim_time_ns_[id]);
    VPageBlock *v_block = v_blocks_[block_num];
    if (v_block->is_unused()) {
      // printf("block %lu unused\n", block_num);
      continue;
    }
    if (!v_block->get_ownership()) {
      //   printf("block %lu is owned\n", block_num);
      continue;
    }

    assert(block_num != client.v_block_number_);
    assert(v_block->is_owned());

    bool compacted = false;
    uint32_t total_used_bytes = 0;
    for (const VPage &v_page : v_block->v_pages) {
      total_used_bytes += v_page.offset;
    }
    const uint32_t modified_threshold = reclaim_percentage_ * total_used_bytes;
    uint32_t modified_bytes = 0;
    // for (const VPage &v_page : v_block->v_pages) {
    for (int pid = 0; pid < v_block->v_pages.size(); pid++) {
      const VPage &v_page = v_block->v_pages[pid];
#ifdef REDUCE_PM_ACCESS
      modified_bytes += block_gcinfo_[block_num].modified_bytes[pid];
#else
      modified_bytes += v_page.modified_bytes;
#endif
      if (modified_bytes > modified_threshold ||
          v_page.next_insert_offset != VPage::DATA_SIZE) {
        compact(client, v_block, block_num);
        compacted = true;
        v_block->clean_version_lock();
        {
          std::lock_guard<std::mutex> lock(reserved_blocks_lock_);
          if (reserved_blocks_.size() < num_reclaim_threads_) {
            reserved_blocks_.push(block_num);
          } else {
            free_blocks_.enqueue(block_num);
          }
        }
        ++local_freed_blocks;
        break;
      }

      // if (v_page.next_insert_offset != VPage::DATA_SIZE) {
      //     // This page has not been completed, so no need to check next one.
      //     break;
      // }
    }
    if (!compacted) {
      v_block->release_ownership();
    }
  }
  // printf("freed %lu\n", local_freed_blocks);
  // local_reclaim_time_ns_[id] += NowMicros() - start_time;
  local_reclaim_blocks_[id] += local_freed_blocks;
  reclaimed_blocks_ += local_freed_blocks;
}

template <typename K, typename V>
void Viper<K, V>::StartCleanStatistics() {
  start_clean_statistics_time = NowMicros();
  for (int i = 0; i < num_reclaim_threads_; i++) {
    local_reclaim_time_before_ns_[i] = local_reclaim_time_ns_[i];
    local_reclaim_blocks_before_[i] = local_reclaim_blocks_[i];
  }
}

template <typename K, typename V>
double Viper<K, V>::GetCompactionCPUUsage() {
  if (v_config_.enable_reclamation) {
    uint64_t total_reclaim_time = 0;
    uint64_t wall_time = NowMicros() - start_clean_statistics_time;
    for (int i = 0; i < num_reclaim_threads_; i++) {
      total_reclaim_time +=
          local_reclaim_time_ns_[i] - local_reclaim_time_before_ns_[i];
    }
    return (double)total_reclaim_time / 1e3 / wall_time;
  } else {
    return 0.0;
  }
}

template <typename K, typename V>
double Viper<K, V>::GetReclaimThroughput() {
  if (v_config_.enable_reclamation) {
    double total_tp = 0.;
    for (int i = 0; i < num_reclaim_threads_; i++) {
      uint64_t reclaim_block =
          local_reclaim_blocks_[i] - local_reclaim_blocks_before_[i];
      uint64_t reclaim_time =
          local_reclaim_time_ns_[i] - local_reclaim_time_before_ns_[i];
      total_tp += (double)reclaim_block * BLOCK_SIZE * 1e6 / reclaim_time;
    }
    return total_tp;
  } else {
    return 0.0;
  }
}

// thread entry
template <typename K, typename V>
void Viper<K, V>::reclaim(int id) {
  // bind_core_on_numa(v_config_.num_client_threads + id);
  Client client = get_reclaim_client();
  client.client_id_ = v_config_.num_client_threads + id;

  uint64_t local_reclaim_epoch = 0;
  while (do_reclaim_.load(LOAD_ORDER) == true) {
    if (local_reclaim_epoch < reclaim_epoch_.load(LOAD_ORDER)) {
      local_reclaim_epoch = reclaim_epoch_.load(LOAD_ORDER);

      reclaim_work(id, client);

      if (++reclaim_num_done_ == num_reclaim_threads_) {
        reclaim_num_done_ = 0;
        uint64_t cur_reclaimed_blocks =
            reclaimed_blocks_.load(LOAD_ORDER) - last_reclaimed_blocks_;
        const block_size_t max_block =
            KVOffset{current_block_page_.load(LOAD_ORDER)}.block_number;
        double cur_percentage = reclaim_percentage_.load(LOAD_ORDER);
        if (cur_reclaimed_blocks < max_block * 0.01) {
          if (cur_percentage > MIN_RECLAIM_FREE_PERCENTAGE) {
            cur_percentage =
                std::max(cur_percentage - 0.05, MIN_RECLAIM_FREE_PERCENTAGE);
            reclaim_percentage_.store(cur_percentage, STORE_ORDER);
          }
        } else if (cur_reclaimed_blocks > max_block * 0.05) {
          if (cur_percentage < MAX_RECLAIM_FREE_PERCENTAGE) {
            cur_percentage =
                std::min(cur_percentage + 0.05, MAX_RECLAIM_FREE_PERCENTAGE);
            reclaim_percentage_.store(cur_percentage, STORE_ORDER);
          }
        }
        DEBUG_LOG("reclaim percentage " << cur_percentage);
        last_reclaimed_blocks_ += cur_reclaimed_blocks;
        is_reclaiming_.store(false, STORE_ORDER);
      }
    } else {
      usleep(10);
    }
  }
}

}  // namespace viper
