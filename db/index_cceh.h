#pragma once

#include "db.h"
#include "index/CCEH/CCEH.h"

class CCEHIndex : public Index {
 public:
  CCEHIndex() { table_ = new CCEH_NAMESPACE::CCEH(128 * 1024); }

  virtual ~CCEHIndex() override { delete table_; }

  virtual ValueType Get(const Slice &key) override {
    return table_->Get(*(KeyType *)key.data());
  }

  virtual void Put(const Slice &key, LogEntryHelper &le_helper) override {
    table_->Insert(*(KeyType *)key.data(), le_helper);
  }

  virtual void GCMove(const Slice &key, LogEntryHelper &le_helper) override {
#ifdef GC_SHORTCUT
    if (le_helper.shortcut.None() ||
        !table_->TryGCUpdate(*(KeyType *)key.data(), le_helper)) {
      table_->Insert(*(KeyType *)key.data(), le_helper);
    }
#else
    table_->Insert(*(KeyType *)key.data(), le_helper);
#endif
  }

  virtual void Delete(const Slice &key) override {
    // TODO
  }

  virtual void PrefetchEntry(const Shortcut &sc) override {
    CCEH_NAMESPACE::Segment *s = (CCEH_NAMESPACE::Segment *)sc.GetNodeAddr();
    __builtin_prefetch(&s->sema);
  }

 private:
  CCEH_NAMESPACE::CCEH *table_;

  DISALLOW_COPY_AND_ASSIGN(CCEHIndex);
};
