#pragma once

#include "db.h"
#include "index/CCEH/CCEH.h"

class CCEHIndex : public Index {
 public:
  CCEHIndex(DB *db):db_(db) { table_ = new CCEH_NAMESPACE::CCEH(128 * 1024); }

  virtual ~CCEHIndex() override { delete table_; }

  virtual ValueType Get(const Slice &key) override {
    return table_->Get(*(KeyType *)key.data());
  }

  virtual void Put(const Slice &key, LogEntryHelper &le_helper) override {
    table_->Insert(*(KeyType *)key.data(), le_helper);
  }

  virtual void GCMove(const Slice &key, LogEntryHelper &le_helper) override {
    KeyType k = *(KeyType *)key.data();
#ifdef GC_SHORTCUT
    if (le_helper.shortcut.None() ||
        !table_->TryGCUpdate(k, le_helper)) {
      table_->Insert(k, le_helper);
    }
#else
#ifdef HOT_SC
    if(le_helper.is_hot_sc)
    {
      ValueType old_val = le_helper.old_val;
      ValueType new_val = le_helper.new_val;
      if(!CAS(&db_->hot_sc[k]->addr, &old_val, new_val))
      {
        le_helper.old_val = le_helper.new_val;
      }
    }
    else
    {
      table_->Insert(k, le_helper);
    }
#else
    table_->Insert(k, le_helper);
#endif
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
  DB *db_;

  DISALLOW_COPY_AND_ASSIGN(CCEHIndex);
};
