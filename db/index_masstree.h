#pragma once

#include "db.h"
#include "index/masstree/masstree_wrapper.h"

class MasstreeIndex : public Index {
 public:
  MasstreeIndex() { mt_ = new MasstreeWrapper(); }

  virtual ~MasstreeIndex() override { delete mt_; }

  void MasstreeThreadInit(int thread_id) { mt_->thread_init(thread_id); }

  virtual ValueType Get(const Slice &key) override {
    ValueType val;
    bool found = mt_->search(*(KeyType *)key.data(), val);
    if (found) {
      return val;
    } else {
      return INVALID_VALUE;
    }
  }

  virtual void Put(const Slice &key, LogEntryHelper &le_helper) override {
    mt_->insert(*(KeyType *)key.data(), le_helper);
  }

  virtual void GCMove(const Slice &key, LogEntryHelper &le_helper) override {
#ifdef GC_SHORTCUT
    if (le_helper.shortcut.None()) {
      mt_->gc_insert(*(KeyType *)key.data(), le_helper);
    } else {
      mt_->gc_insert_with_shortcut(*(KeyType *)key.data(), le_helper);
    }
#else
    mt_->gc_insert(*(KeyType *)key.data(), le_helper);
#endif
  }

  virtual void Delete(const Slice &key) override {
    // TODO
  }

  virtual void Scan(const Slice &key, int cnt,
                    std::vector<ValueType> &vec) override {
    mt_->scan(*(KeyType *)key.data(), cnt, vec);
  }

  // virtual void PrefetchEntry(const Shortcut &sc) override {}

 private:
  MasstreeWrapper *mt_;

  DISALLOW_COPY_AND_ASSIGN(MasstreeIndex);
};
