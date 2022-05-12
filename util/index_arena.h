#pragma once

#include "util/util.h"
#include "util/debug_helper.h"
#include "util/lock.h"
#include "config.h"

#include <libpmemobj.h>
#include <string>
#include <unordered_map>
#include <mutex>
#include <queue>
#include <atomic>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstdio>


// static constexpr uint64_t IDX_CHUNK_SIZE = 64UL << 20;

class IndexAllocator {
 public:
  const std::string idx_pool_path_;
  const size_t idx_pool_size_;
  SpinLock lock_;
  std::unordered_map<size_t, std::queue<void *>> reserved_space_;
  IndexAllocator(std::string pool_path, size_t pool_size)
      : idx_pool_path_(pool_path), idx_pool_size_(pool_size) {}
  virtual ~IndexAllocator() = default;
  virtual void *Alloc(size_t size) = 0;
  virtual void Free(void *ptr, size_t size) = 0;
  virtual void Initialize() = 0;
  virtual size_t ToPoolOffset(const char *addr) = 0;
  virtual void *ToDirectPointer(size_t offset) = 0;
  // virtual char *GetStartAddr() = 0;

  DISALLOW_COPY_AND_ASSIGN(IndexAllocator);
};


class PMDKAllocator : public IndexAllocator {
 public:
  PMDKAllocator(std::string pool_path, size_t pool_size)
      : IndexAllocator(pool_path, pool_size) {
    Initialize();
  }

  virtual ~PMDKAllocator() {
    if (pop) {
      pmemobj_close(pop);
    }
  }

  virtual void *Alloc(size_t size) override {
    std::lock_guard<SpinLock> lk_(lock_);
    if (reserved_space_.find(size) != reserved_space_.end() &&
        !reserved_space_[size].empty()) {
      void *ret = reserved_space_[size].front();
      reserved_space_[size].pop();
      return ret;
    }

    PMEMoid oid;
    int ret = pmemobj_alloc(pop, &oid, size, 0, nullptr, nullptr);
    if (ret != 0) {
      ERROR_EXIT("PMem allocate failed.");
    }
    return pmemobj_direct(oid);
  }

  virtual void Free(void *ptr, size_t size) override {
    reserved_space_[size].push(ptr);
    // PMEMoid oid = pmemobj_oid(ptr);
    // pmemobj_free(&oid);
  }

  virtual void Initialize() override {
    Destroy();
    reserved_space_.clear();

    pop = pmemobj_create(idx_pool_path_.c_str(), "IDX", idx_pool_size_, 0664);
    if (pop == nullptr) {
      ERROR_EXIT("Create PMEMobjpool failed.");
    }
  }

  virtual size_t ToPoolOffset(const char *addr) override {
    return (size_t)addr - (size_t)pop;
  }

  virtual void *ToDirectPointer(size_t offset) override {
    return (void *)((size_t)pop + offset);
  }

  // virtual char *GetStartAddr() override { return (char *)pop; }

 private:
  void Destroy() {
    if (pop) {
      pmemobj_close(pop);
      pop = nullptr;
      remove(idx_pool_path_.c_str());
    }
  }

  PMEMobjpool *pop = nullptr;

  DISALLOW_COPY_AND_ASSIGN(PMDKAllocator);
};


class MMAPAllocator : public IndexAllocator {
 public:
  MMAPAllocator(std::string pool_path, size_t pool_size)
      : IndexAllocator(pool_path, pool_size) {
    Initialize();
  }

  virtual ~MMAPAllocator() override {
    if (idx_start_addr_) {
      munmap(idx_start_addr_, idx_pool_size_);
    }
  }

  virtual void *Alloc(size_t size) override {
    std::lock_guard<SpinLock> lk_(lock_);
    if (reserved_space_.find(size) != reserved_space_.end() &&
        !reserved_space_[size].empty()) {
      void *ret = reserved_space_[size].front();
      reserved_space_[size].pop();
      return ret;
    }

    cur_alloc_addr_ =
        (char *)((uintptr_t)(cur_alloc_addr_ + CACHE_LINE_SIZE - 1) &
                 ~(uintptr_t)(CACHE_LINE_SIZE - 1));
    void *ret = cur_alloc_addr_;
    cur_alloc_addr_ += size;
    if (cur_alloc_addr_ > pool_end_addr_) {
      ERROR_EXIT("No enough index_pool space.");
    }
    return ret;
  }

  virtual void Free(void *ptr, size_t size) override {
    if (ptr == nullptr) {
      ERROR_EXIT("free nullptr");
    } else {
      reserved_space_[size].push(ptr);
    }
  }

  virtual void Initialize() override {
    if (idx_start_addr_) {
      munmap(idx_start_addr_, idx_pool_size_);
    }
    reserved_space_.clear();

#ifdef IDX_PERSISTENT
    int idx_pool_fd = open(idx_pool_path_.c_str(), O_CREAT | O_RDWR, 0644);
    if (idx_pool_fd < 0) {
      ERROR_EXIT("open file failed");
    }
    if (fallocate(idx_pool_fd, 0, 0, idx_pool_size_) != 0) {
      ERROR_EXIT("fallocate file failed");
    }
    idx_start_addr_ = (char *)mmap(NULL, idx_pool_size_, PROT_READ | PROT_WRITE,
                                   MAP_SHARED, idx_pool_fd, 0);
    close(idx_pool_fd);
#else
    idx_start_addr_ = (char *)mmap(NULL, idx_pool_size_, PROT_READ | PROT_WRITE,
                                   MAP_SHARED | MAP_ANONYMOUS, -1, 0);
#endif
    if (idx_start_addr_ == nullptr || idx_start_addr_ == MAP_FAILED) {
      ERROR_EXIT("idx_pool mmap failed");
    }
    pool_end_addr_ = idx_start_addr_ + idx_pool_size_;
    cur_alloc_addr_ = idx_start_addr_;
  }

  virtual size_t ToPoolOffset(const char *addr) override {
    return (size_t)addr - (size_t)idx_start_addr_;
  }

  virtual void *ToDirectPointer(size_t offset) override {
    return (void *)((size_t)idx_start_addr_ + offset);
  }

  // virtual char *GetStartAddr() override { return idx_start_addr_; }

 private:
  char *idx_start_addr_ = nullptr;
  char *pool_end_addr_ = nullptr;
  char * volatile cur_alloc_addr_ = nullptr;

  // void GetNewChunk() {
  //   cur_addr = global_alloc_addr.fetch_add(IDX_CHUNK_SIZE);
  //   if (cur_addr > idx_start_addr_ + idx_pool_size_) {
  //     ERROR_EXIT("No enough index_pool space.");
  //   }
  //   end_addr = cur_addr + IDX_CHUNK_SIZE;
  // }
  DISALLOW_COPY_AND_ASSIGN(MMAPAllocator);
};


extern IndexAllocator *g_index_allocator;
