#include "db.h"
#include "db_common.h"
#include "log_structured.h"
#include "hotkeyset.h"

#if INDEX_TYPE <= 1
#include "index_cceh.h"
#elif INDEX_TYPE == 2
#include "index_fastfair.h"
#elif INDEX_TYPE == 3
#include "index_masstree.h"
#else
static_assert(false, "error index kind");
#endif

// class DB

DB::DB(std::string db_path, size_t log_size, int num_workers, int num_cleaners)
    : num_workers_(num_workers),
      num_cleaners_(num_cleaners),
      thread_status_(num_workers) {
#if defined(USE_PMDK) && defined(IDX_PERSISTENT)
  g_index_allocator = new PMDKAllocator(db_path + "/idx_pool", IDX_POOL_SIZE);
#else
  g_index_allocator = new MMAPAllocator(db_path + "/idx_pool", IDX_POOL_SIZE);
#endif

#if INDEX_TYPE <= 1
  index_ = new CCEHIndex();
  printf("DB index type: CCEH\n");
#elif INDEX_TYPE == 2
  index_ = new FastFairIndex();
  printf("DB index type: FastFair\n");
#elif INDEX_TYPE == 3
  index_ = new MasstreeIndex();
  printf("DB index type: Masstree\n");
#endif

#ifdef HOT_COLD_SEPARATE
  hot_key_set_ = new HotKeySet(this);
#endif
  // init log-structured
  log_ =
      new LogStructured(db_path, log_size, this, num_workers_, num_cleaners_);
};

DB::~DB() {
  delete log_;
  delete index_;
  delete g_index_allocator;
  g_index_allocator = nullptr;
  if (cur_num_workers_.load() != 0) {
    ERROR_EXIT("%d worker(s) not ending", cur_num_workers_.load());
  }
#ifdef HOT_COLD_SEPARATE
  delete hot_key_set_;
  hot_key_set_ = nullptr;
#endif
}

void DB::StartCleanStatistics() { log_->StartCleanStatistics(); }

double DB::GetCompactionCPUUsage() { return log_->GetCompactionCPUUsage(); }

double DB::GetCompactionThroughput() { return log_->GetCompactionThroughput(); }

void DB::RecoverySegments() {
  if (cur_num_workers_.load() != 0) {
    ERROR_EXIT("%d worker(s) not ending", cur_num_workers_.load());
  }
  log_->RecoverySegments(this);
}

void DB::RecoveryInfo() {
  if (cur_num_workers_.load() != 0) {
    ERROR_EXIT("%d worker(s) not ending", cur_num_workers_.load());
  }
  log_->RecoveryInfo(this);
}

void DB::RecoveryAll() {
  if (cur_num_workers_.load() != 0) {
    ERROR_EXIT("%d worker(s) not ending", cur_num_workers_.load());
  }
  log_->RecoveryAll(this);
}

void DB::NewIndexForRecoveryTest() {
  delete index_;
  g_index_allocator->Initialize();

#if INDEX_TYPE <= 1
  index_ = new CCEHIndex();
#elif INDEX_TYPE == 2
  index_ = new FastFairIndex();
#elif INDEX_TYPE == 3
  index_ = new MasstreeIndex();
#endif
}

// class DB::Worker

DB::Worker::Worker(DB *db) : db_(db) {
  worker_id_ = db_->cur_num_workers_.fetch_add(1);
  tmp_cleaner_garbage_bytes_.resize(db_->num_cleaners_, 0);
#if INDEX_TYPE == 3
  reinterpret_cast<MasstreeIndex *>(db_->index_)
      ->MasstreeThreadInit(worker_id_);
#endif
}

DB::Worker::~Worker() {
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

bool DB::Worker::Get(const Slice &key, std::string *value) {
  db_->thread_status_.rcu_progress(worker_id_);
  ValueType val = db_->index_->Get(key);
  bool ret = false;
  if (val != INVALID_VALUE) {
    TaggedPointer(val).GetKVItem()->GetValue(*value);
    ret = true;
  }
  db_->thread_status_.rcu_exit(worker_id_);
  return ret;
}

void DB::Worker::Put(const Slice &key, const Slice &value) {
  bool hot;
#ifdef HOT_COLD_SEPARATE
  hot = db_->hot_key_set_->Exist(key);
#else
  hot = true;
#endif
  ValueType val = MakeKVItem(key, value, hot);
#ifndef LOG_BATCHING
  UpdateIndex(key, val, hot);
#endif
}

size_t DB::Worker::Scan(const Slice &key, int cnt) {
  db_->thread_status_.rcu_progress(worker_id_);
  std::vector<ValueType> vec;
  db_->index_->Scan(key, cnt, vec);
  std::string s_value;
  for (int i = 0; i < vec.size(); i++) {
    ValueType val = vec[i];
    TaggedPointer(val).GetKVItem()->GetValue(s_value);
  }
  db_->thread_status_.rcu_exit(worker_id_);
  return vec.size();
}

bool DB::Worker::Delete(const Slice &key) { ERROR_EXIT("not implemented yet"); }

#ifdef LOG_BATCHING
void DB::Worker::BatchIndexInsert(int cnt, bool hot) {
#ifdef HOT_COLD_SEPARATE
  std::queue<std::pair<KeyType, ValueType>> &queue =
      hot ? buffer_queue_ : cold_buffer_queue_;
#else
  std::queue<std::pair<KeyType, ValueType>> &queue = buffer_queue_;
#endif
  while (cnt--) {
    std::pair<KeyType, ValueType> kv_pair = queue.front();
    UpdateIndex(Slice((const char *)&kv_pair.first, sizeof(KeyType)),
                kv_pair.second, hot);
    queue.pop();
  }
}

ValueType DB::Worker::MakeKVItem(const Slice &key, const Slice &value,
                                 bool hot) {
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
         (ret = segment->AppendBatchFlush(key, value, epoch, &persist_cnt)) ==
             INVALID_VALUE) {
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

  assert(ret);
  return ret;
}
#else
ValueType DB::Worker::MakeKVItem(const Slice &key, const Slice &value,
                                 bool hot) {
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

void DB::Worker::UpdateIndex(const Slice &key, ValueType val, bool hot) {
  LogEntryHelper le_helper(val);
  db_->index_->Put(key, le_helper);
#ifdef GC_SHORTCUT
#ifdef HOT_COLD_SEPARATE
  if (hot) {
    log_head_->AddShortcut(le_helper.shortcut);
  } else {
    cold_log_head_->AddShortcut(le_helper.shortcut);
  }
#else
  log_head_->AddShortcut(le_helper.shortcut);
#endif
#endif

  // mark old garbage
  if (le_helper.old_val != INVALID_VALUE) {
#ifdef HOT_COLD_SEPARATE
    db_->thread_status_.rcu_progress(worker_id_);
    db_->hot_key_set_->Record(key, worker_id_, hot);
    db_->thread_status_.rcu_exit(worker_id_);
#endif
    MarkGarbage(le_helper.old_val);
  }
}

void DB::Worker::MarkGarbage(ValueType tagged_val) {
  TaggedPointer tp(tagged_val);
#ifdef REDUCE_PM_ACCESS
  uint32_t sz = tp.size;
  if (sz == 0) {
    ERROR_EXIT("size == 0");
    KVItem *kv = tp.GetKVItem();
    sz = sizeof(KVItem) + kv->key_size + kv->val_size;
  }
#else
  KVItem *kv = tp.GetKVItem();
  uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
#endif
  int segment_id = db_->log_->GetSegmentID(tp.GetAddr());
  LogSegment *segment = db_->log_->GetSegment(segment_id);
  segment->MarkGarbage(tp.GetAddr(), sz);
  // update temp cleaner garbage bytes
  if (db_->num_cleaners_ > 0) {
    int cleaner_id = db_->log_->GetSegmentCleanerID(tp.GetAddr());
    tmp_cleaner_garbage_bytes_[cleaner_id] += sz;
  }
}

void DB::Worker::FreezeSegment(LogSegment *segment) {
  db_->log_->FreezeSegment(segment);
  db_->log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
}
