#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <queue>

#include "hotkeyset.h"
#include "util/thread_status.h"
#include "util/util.h"

#include "ChameleonIndex.h"
#include "log.h"

namespace CHAMELEONDB_NAMESPACE {

class ChameleonDB {
 public:
  class Worker {
   public:
    Worker(ChameleonDB *db) : db_(db) {
      worker_id_ = db_->max_worker_id_.fetch_add(1);
      db_->cur_num_workers_.fetch_add(1);
    }

    ~Worker() {
#ifdef LOG_BATCHING
      BatchIndexInsert(buffer_queue_.size(), true);
#ifdef HOT_COLD_SEPARATE
      BatchIndexInsert(cold_buffer_queue_.size(), false);
#endif
#endif
      FreezeSegment(log_head_);
      log_head_ = nullptr;
#ifdef HOT_COLD_SEPARATE
      FreezeSegment(cold_log_head_);
      cold_log_head_ = nullptr;
#endif
      db_->cur_num_workers_--;
    }

    bool Get(const Slice &key, std::string *value) {
      db_->thread_status_.rcu_progress(worker_id_);
      ValueType val = db_->IndexGet(key);
      bool ret = false;
      if (val != INVALID_VALUE) {
        TaggedPointer(val).GetKVItem()->GetValue(*value);
        ret = true;
      }
      db_->thread_status_.rcu_exit(worker_id_);
      return ret;
    }

    void Put(const Slice &key, const Slice &value) {
      bool hot;
#ifdef HOT_COLD_SEPARATE
      hot = db_->hot_key_set_->Exist(key);
#else
      hot = true;
#endif
      ValueType val = MakeKVItem(key, value, hot);
#ifndef LOG_BATCHING
      db_->IndexPut(key, val);
#endif
    }

    bool Delete(const Slice &key) { ERROR_EXIT("not implemented yet"); }

#ifdef LOG_BATCHING
    void BatchIndexInsert(int cnt, bool hot) {
      bool hit = false;
#ifdef HOT_COLD_SEPARATE
      LogSegment *&segment = hot ? log_head_ : cold_log_head_;
      std::queue<std::pair<KeyType, ValueType>> &queue =
          hot ? buffer_queue_ : cold_buffer_queue_;
#else
      LogSegment *&segment = log_head_;
      std::queue<std::pair<KeyType, ValueType>> &queue = buffer_queue_;
#endif
      while (cnt--) {
        std::pair<KeyType, ValueType> kv_pair = queue.front();
        db_->IndexPut(Slice((const char *)&kv_pair.first, sizeof(KeyType)),
                      kv_pair.second);
        queue.pop();
      }
    }

    ValueType MakeKVItem(const Slice &key, const Slice &value, bool hot) {
      ValueType ret = INVALID_VALUE;
      uint64_t i_key = *(uint64_t *)key.data();
      uint32_t epoch = db_->GetKeyEpoch(i_key);
#ifdef HOT_COLD_SEPARATE
      LogSegment *&segment = hot ? log_head_ : cold_log_head_;
      std::queue<std::pair<KeyType, ValueType>> &queue =
          hot ? buffer_queue_ : cold_buffer_queue_;
#else
      LogSegment *&segment = log_head_;
      std::queue<std::pair<KeyType, ValueType>> &queue = buffer_queue_;
#endif
      int persist_cnt = 0;
      while (segment == nullptr ||
             (ret = segment->AppendBatchFlush(key, value, epoch,
                                              &persist_cnt)) == INVALID_VALUE) {
        if (segment) {
          persist_cnt = segment->FlushRemain();
          BatchIndexInsert(persist_cnt, hot);
          FreezeSegment(segment);
        }
        segment = db_->log_->NewSegment(hot);
      }
      queue.push({i_key, ret});
      if (persist_cnt > 0) {
        BatchIndexInsert(persist_cnt, hot);
      }

      assert(ret == 0);
      return ret;
    }
#else
    ValueType MakeKVItem(const Slice &key, const Slice &value, bool hot) {
      ValueType ret = INVALID_VALUE;
      uint64_t i_key = *(uint64_t *)key.data();
      uint32_t epoch = db_->GetKeyEpoch(i_key);

#ifdef HOT_COLD_SEPARATE
      LogSegment *&segment = hot ? log_head_ : cold_log_head_;
#else
      LogSegment *&segment = log_head_;
#endif

      while (segment == nullptr ||
             (ret = segment->Append(key, value, epoch)) == INVALID_VALUE) {
        FreezeSegment(segment);
        segment = db_->log_->NewSegment(hot);
      }
      assert(ret);
      return ret;
    }
#endif

   private:
    ChameleonDB *db_;
    int worker_id_;
    LogSegment *log_head_ = nullptr;
#ifdef HOT_COLD_SEPARATE
    LogSegment *cold_log_head_ = nullptr;
#endif

    void FreezeSegment(LogSegment *segment) {
      db_->log_->FreezeSegment(segment);
    }

#ifdef LOG_BATCHING
    std::queue<std::pair<KeyType, ValueType>> buffer_queue_;
#ifdef HOT_COLD_SEPARATE
    std::queue<std::pair<KeyType, ValueType>> cold_buffer_queue_;
#endif

#endif

    DISALLOW_COPY_AND_ASSIGN(Worker);
  };

  ChameleonDB(std::string db_path, size_t log_size, int num_workers,
              int num_cleaners)
      : num_workers_(num_workers),
        num_cleaners_(num_cleaners),
        thread_status_(num_workers) {
#ifdef USE_PMDK
    g_index_allocator =
        new PMDKAllocator(db_path + "/idx_pool", IDX_POOL_SIZE);
#else
    g_index_allocator =
        new MMAPAllocator(db_path + "/idx_pool", IDX_POOL_SIZE);
#endif
    chameleon_index_ = new CHAMELEONDB_NAMESPACE::ChameleonIndex();
#ifdef HOT_COLD_SEPARATE
    hot_key_set_ = new HotKeySet(this);
#endif
    log_ = new Log(db_path, log_size, this, num_workers, num_cleaners_);
  }

  ~ChameleonDB() {
    if (cur_num_workers_.load() != 0) {
      ERROR_EXIT("%d worker(s) not ending", cur_num_workers_.load());
    }
#ifdef HOT_COLD_SEPARATE
    delete hot_key_set_;
    hot_key_set_ = nullptr;
#endif
    delete log_;
    delete chameleon_index_;
    delete g_index_allocator;
    g_index_allocator = nullptr;
  }

  std::unique_ptr<Worker> GetWorker() {
    return std::make_unique<Worker>(this);
  }

 private:
  const int num_workers_;
  const int num_cleaners_;
  std::atomic<int> cur_num_workers_{0};
  std::atomic<int> max_worker_id_{0};
  Log *log_;
#ifdef HOT_COLD_SEPARATE
  HotKeySet *hot_key_set_ = nullptr;
#endif
  ThreadStatus thread_status_;

  CHAMELEONDB_NAMESPACE::ChameleonIndex *chameleon_index_;

  static constexpr int EPOCH_MAP_SIZE = 1024;
  std::array<std::atomic<uint_fast32_t>, EPOCH_MAP_SIZE> epoch_map_{};

  uint32_t GetKeyEpoch(uint64_t i_key) {
    size_t idx = i_key % EPOCH_MAP_SIZE;
    return epoch_map_[idx].fetch_add(1, std::memory_order_relaxed);
  }

  ValueType IndexGet(const Slice &key) {
    ValueType val;
    if (chameleon_index_->Get(*(KeyType *)key.data(), &val)) {
      return val;
    } else {
      return INVALID_VALUE;
    }
  }

  void IndexPut(const Slice &key, const ValueType &val) {
    chameleon_index_->Put(*(KeyType *)key.data(), val);
  }

  void IndexDelete(const Slice &key) {
    // TODO
  }

  bool LockIfValid(const KeyType key, const ValueType val) {
    return chameleon_index_->LockIfValid(key, val);
  }

  bool IfValid(const KeyType key, const ValueType val) {
    return chameleon_index_->IfValid(key, val);
  }

  void GCMoveAndUnlock(const KeyType key, const ValueType new_val) {
    chameleon_index_->GCMoveAndUnlock(key, new_val);
  }

  friend class LogGCer;
  friend class HotKeySet;
  DISALLOW_COPY_AND_ASSIGN(ChameleonDB);
};

}  // namespace CHAMELEONDB_NAMESPACE
