#include "ChameleonIndex.h"

#include <libpmem.h>
#include <random>

namespace CHAMELEONDB_NAMESPACE {

void Shard::Init(int shard_id, ShardMeta *meta, size_t base_seed) {
  shard_id_ = shard_id;
  meta_ = meta;
  unsigned seed = base_seed;
  // std::random_device rd;
  std::default_random_engine gen(1122);
  std::uniform_real_distribution<double> dis(kLoadFactorLow, kLoadFactorHigh);
  load_factor_ = dis(gen);
  // MemTable
  void *memtable_records = malloc(sizeof(KVPair) * kNumMemTableSlot);
  memtable_ = new HashTable();
  memtable_->Init(memtable_records, kNumMemTableSlot,
                  [base_seed](const Key_t &key) {
                    return hash_funcs[0](&key, sizeof(Key_t), base_seed);
                  });
  // ABI
  void *ABI_records = malloc(sizeof(KVPair) * kNumABISlot);
  ABI_ = new HashTable();
  ABI_->Init(ABI_records, kNumABISlot, [base_seed](const Key_t &key) {
    return hash_funcs[0](&key, sizeof(Key_t), base_seed + 0x1234);
  });
  // Interlayer
  size_t table_num_slots = kNumMemTableSlot;
  for (int i = 0; i < kNumLevels - 1; ++i) {
    table_count_[i] = 0;
    for (int j = 0; j < kBetweenLevelRatio - 1; ++j) {
      void *table_records =
          g_index_allocator->Alloc(sizeof(KVPair) * table_num_slots);
      interlayer_tables_[i][j].records_ = (KVPair *)table_records;
      interlayer_tables_[i][j].num_slots_ = table_num_slots;
      interlayer_tables_[i][j].cur_num_ = 0;
      interlayer_tables_[i][j].hash_func = [i, base_seed](const Key_t &key) {
        return hash_funcs[i](&key, sizeof(Key_t), base_seed);
      };
    }
    table_num_slots *= kBetweenLevelRatio;
  }
  // last level table
  last_level_table_ = new HashTable();
  last_level_table_->records_ = nullptr;
  last_level_table_->num_slots_ = table_num_slots;
  last_level_table_->cur_num_ = 0;
  last_level_table_->hash_func = [base_seed](const Key_t &key) {
    return hash_funcs[kNumLevels - 1](&key, sizeof(Key_t), base_seed);
  };
}

Shard::~Shard() {
  free(memtable_->records_);
  delete memtable_;
  free(ABI_->records_);
  delete ABI_;
  delete last_level_table_;
}

bool Shard::Get(const Key_t &key, Value_t *val) {
  ReadLockHelper rh(rwlock_);
  // 1. MemTable
  assert(memtable_ && memtable_->records_);
  if (memtable_->Get(key, val)) {
    return true;
  }

  // 1.5 immutable MemTable
  if (immutable_memtable_ && immutable_memtable_->Get(key, val)) {
    return true;
  }

  // 2. ABI
  if (ABI_->Get(key, val)) {
    return true;
  }

  // 3. last level
  if (last_level_table_->Get(key, val)) {
    return true;
  }

  return false;
}

void Shard::Put(const Key_t &key, const Value_t &val) {
  // gc_lock_.ReadLock();
  rwlock_.ReadLock();
  bool locked = true;
  HashTable *mt = memtable_;

  assert(mt && mt->records_);
  bool res = mt->Put(key, val);

  while (!res || mt->CurLoadFactor() >= load_factor_) {
    // flush
    rwlock_.ReadUnlock();
    locked = false;
    if (!compact_flag.test_and_set()) {
      // succeed
      if (mt == memtable_) {
        FlushOrCompact(mt);
      }
      compact_flag.clear();
    }
    if (!res) {
      // insert failed
      while (mt == memtable_) {
        asm("nop");
      }
      // retry
      rwlock_.ReadLock();
      locked = true;
      mt = memtable_;
      res = mt->Put(key, val);
    } else {
      break;
    }
  }
  if (locked) {
    rwlock_.ReadUnlock();
  }
  // gc_lock_.ReadUnlock();
}

void Shard::GCPut(const Key_t &key, const Value_t &val) {
  // should hold gc_lock_ write lock !
  rwlock_.ReadLock();
  bool locked = true;
  HashTable *mt = memtable_;

  assert(mt && mt->records_);
  bool res = mt->Put(key, val);

  while (!res || mt->CurLoadFactor() >= load_factor_) {
    // flush
    rwlock_.ReadUnlock();
    locked = false;
    if (!compact_flag.test_and_set()) {
      // succeed
      if (mt == memtable_) {
        FlushOrCompact(mt);
      }
      compact_flag.clear();
    }
    if (!res) {
      // insert failed
      while (mt == memtable_) {
        asm("nop");
      }
      // retry
      rwlock_.ReadLock();
      locked = true;
      mt = memtable_;
      res = mt->Put(key, val);
    } else {
      break;
    }
  }
  if (locked) {
    rwlock_.ReadUnlock();
  }
}

void Shard::UpdateMeta() {
  ShardMeta m;
  m.L0CNT = table_count_[0];
  m.L1CNT = table_count_[1];
  m.L2CNT = table_count_[2];
  m.L3ADDR = (uint64_t)last_level_table_->records_;
  *meta_ = m;
  clwb_fence(meta_, sizeof(ShardMeta));
}

void Shard::FlushOrCompact(HashTable *old_mt) {
  void *memtable_records = malloc(sizeof(KVPair) * kNumMemTableSlot);
  HashTable *new_mt = new HashTable();
  new_mt->Init(memtable_records, kNumMemTableSlot, old_mt->hash_func);
  // ensure no one will write to old MemTable
  rwlock_.WriteLock();
  immutable_memtable_ = old_mt;
  memtable_ = new_mt;
  rwlock_.WriteUnlock();

  if (table_count_[0] < kBetweenLevelRatio - 1) {
    FlushMemTable(old_mt);
  } else {
    Compact(old_mt);
  }

  rwlock_.WriteLock();
  immutable_memtable_ = nullptr;
  rwlock_.WriteUnlock();
  free(old_mt->records_);
  delete old_mt;
}

void Shard::FlushMemTable(HashTable *old_mt) {
  // flush can use ntcopy
  HashTable *dest_table = &interlayer_tables_[0][table_count_[0]];
  pmem_memcpy_persist(dest_table->records_, old_mt->records_, kMemTableSize);
  dest_table->cur_num_ = old_mt->cur_num_;
  ++table_count_[0];
  // copy to ABI
  for (size_t i = 0; i < old_mt->num_slots_; ++i) {
    if (old_mt->records_[i].key != INVALID) {
      ABI_->ForcePut(old_mt->records_[i].key, old_mt->records_[i].val);
    }
  }
  UpdateMeta();
}

void Shard::Compact(HashTable *old_mt) {
  unsigned dest_level = 1;
  unsigned dest_table_cnt = 0;

  // from L1 to Ln-1
  for (unsigned i = 1; i < kNumLevels - 1; ++i) {
    if (table_count_[i] == kBetweenLevelRatio - 1) {
      ++dest_level;
    } else {
      dest_table_cnt = table_count_[i];
      break;
    }
  }

  if (dest_level == kNumLevels - 1) {
    FullCompact(old_mt);
    // random new load factor
    std::random_device rd;
    std::default_random_engine gen(rd());
    std::uniform_real_distribution<double> dis(kLoadFactorLow, kLoadFactorHigh);
    load_factor_ = dis(gen);
  } else {
    PartialCompact(old_mt, dest_level, dest_table_cnt);
  }
}

void Shard::FullCompact(HashTable *old_mt) {
  // all level compaction, merge current last level, ABI, and MemTable
  HashTable *old_abi = ABI_;
  HashTable *old_llt = last_level_table_;
  HashTable *new_llt = new HashTable();
  KVPair *tmp_records = nullptr;
  size_t dest_num_slots;
  double tmp_load_factor =
      (double)(old_llt->cur_num_ + old_abi->cur_num_) / old_llt->num_slots_;

  // 1. re-insert old last level
  if (tmp_load_factor > 0.85) {
    // resize
    dest_num_slots = 2 * old_llt->num_slots_;
    tmp_records = (KVPair *)malloc(dest_num_slots * sizeof(KVPair));
    new_llt->Init(tmp_records, dest_num_slots, old_llt->hash_func);
    if (old_llt->records_ != nullptr) {
      for (size_t i = 0; i < old_llt->num_slots_; ++i) {
        if (old_llt->records_[i].key != INVALID) {
          new_llt->ForcePut(old_llt->records_[i].key, old_llt->records_[i].val);
        }
      }
    }
  } else {
    // same size
    dest_num_slots = old_llt->num_slots_;
    tmp_records = (KVPair *)malloc(dest_num_slots * sizeof(KVPair));
    new_llt->Init(tmp_records, dest_num_slots, old_llt->hash_func);
    if (old_llt->records_ != nullptr) {
      memcpy(new_llt->records_, old_llt->records_,
             dest_num_slots * sizeof(KVPair));
      new_llt->cur_num_ = old_llt->cur_num_;
    }
  }

  // 2. insert ABI
  for (size_t i = 0; i < old_abi->num_slots_; ++i) {
    if (old_abi->records_[i].key != INVALID) {
      new_llt->ForcePut(old_abi->records_[i].key, old_abi->records_[i].val);
    }
  }

  // 3. insert MemTable
  for (size_t i = 0; i < old_mt->num_slots_; ++i) {
    if (old_mt->records_[i].key != INVALID) {
      new_llt->ForcePut(old_mt->records_[i].key, old_mt->records_[i].val);
    }
  }

  // 4. copy to PM
  KVPair *pm_records =
      (KVPair *)g_index_allocator->Alloc(dest_num_slots * sizeof(KVPair));
  pmem_memcpy_persist(pm_records, tmp_records, dest_num_slots * sizeof(KVPair));
  // change PM records_
  new_llt->records_ = pm_records;
  free(tmp_records);

  // 5. update last level and ABI, clear interlayer
  void *ABI_records = malloc(sizeof(KVPair) * kNumABISlot);
  HashTable *new_ABI = new HashTable();
  new_ABI->Init(ABI_records, kNumABISlot, old_abi->hash_func);

  rwlock_.WriteLock();
  last_level_table_ = new_llt;
  ABI_ = new_ABI;
  rwlock_.WriteUnlock();

  for (unsigned i = 0; i < kNumLevels - 1; ++i) {
    table_count_[i] = 0;
    for (unsigned j = 0; j < kBetweenLevelRatio - 1; ++j) {
      interlayer_tables_[i][j].cur_num_ = 0;
    }
  }

  UpdateMeta();

  // 6. delete old last level and old ABI
  if (old_llt->records_ != nullptr) {
    g_index_allocator->Free(old_llt->records_,
                            old_llt->num_slots_ * sizeof(KVPair));
  }
  delete old_llt;
  free(old_abi->records_);
  delete old_abi;
}

void Shard::PartialCompact(HashTable *old_mt, int dest_level,
                           int dest_table_cnt) {
  HashTable *dest_table = &interlayer_tables_[dest_level][dest_table_cnt];

  HashTable tmp_table;
  KVPair *tmp_records =
      (KVPair *)malloc(dest_table->num_slots_ * sizeof(KVPair));
  tmp_table.Init(tmp_records, dest_table->num_slots_, dest_table->hash_func);
  // from old to new
  for (int i = dest_level - 1; i >= 0; --i) {
    for (int j = 0; j < kBetweenLevelRatio - 1; ++j) {
      HashTable *cur_table = &interlayer_tables_[i][j];
      for (size_t k = 0; k < cur_table->num_slots_; ++k) {
        if (cur_table->records_[k].key != INVALID) {
          tmp_table.ForcePut(cur_table->records_[k].key,
                             cur_table->records_[k].val);
        }
      }
    }
  }
  // MemTable, both new table and ABI
  for (size_t k = 0; k < old_mt->num_slots_; ++k) {
    if (old_mt->records_[k].key != INVALID) {
      tmp_table.ForcePut(old_mt->records_[k].key, old_mt->records_[k].val);
      ABI_->ForcePut(old_mt->records_[k].key, old_mt->records_[k].val);
    }
  }

  // copy to real table
  pmem_memcpy_persist(dest_table->records_, tmp_table.records_,
                      dest_table->num_slots_ * sizeof(KVPair));
  dest_table->cur_num_ = tmp_table.cur_num_;

  // update meta
  for (unsigned i = 0; i < dest_level; ++i) {
    table_count_[i] = 0;
  }
  table_count_[dest_level] = dest_table_cnt + 1;

  free(tmp_records);
  UpdateMeta();
}

}  // namespace CHAMELEONDB_NAMESPACE
