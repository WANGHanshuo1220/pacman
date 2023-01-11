#pragma once

#include "db.h"
#include "index/FAST_FAIR/ff_btree.h"

class FastFairIndex : public Index {
 public:
  FastFairIndex() { bt_ = new btree(); }

  virtual ~FastFairIndex() override { delete bt_; }

  virtual ValueType Get(const Slice &key) override {
    return (ValueType)bt_->btree_search(*(KeyType *)key.data());
  }

  virtual void Put(const Slice &key, LogEntryHelper &le_helper) override {
    bt_->btree_insert(*(KeyType *)key.data(), le_helper);
  }

  virtual void GCMove(const Slice &key, LogEntryHelper &le_helper) override {
#ifdef GC_SHORTCUT
    if (le_helper.shortcut.None() ||
        !bt_->btree_try_update(*(KeyType *)key.data(), le_helper)) {
      bt_->btree_insert(*(KeyType *)key.data(), le_helper);
    }
#else
    bt_->btree_insert(*(KeyType *)key.data(), le_helper);
#endif

// #ifdef GC_SHORTCUT
// #ifdef HOT_SC
//     if(le_helper.is_hot_sc)
//     {
//       ValueType old_val = le_helper.old_val;
//       ValueType new_val = le_helper.new_val;
//       if(!CAS(&(*(db_->hot_sc))[k]->addr, &old_val, new_val))
//       {
//         le_helper.old_val = le_helper.new_val;
//       }
//     }
//     else
//     {
//       if (le_helper.shortcut.None() ||
//           !bt_->TryGCUpdate(k, le_helper)) {
//         bt_->Insert(k, le_helper);
//       }
//     }
// #else
//     if (le_helper.shortcut.None() ||
//         !bt_->TryGCUpdate(k, le_helper)) {
//       bt_->Insert(k, le_helper);
//     }
// #endif
// #else
// #ifdef HOT_SC
//     if(le_helper.is_hot_sc)
//     {
//       ValueType old_val = le_helper.old_val;
//       ValueType new_val = le_helper.new_val;
//       if(!CAS(&(*(db_->hot_sc))[k]->addr, &old_val, new_val))
//       {
//         le_helper.old_val = le_helper.new_val;
//       }
//     }
//     else
//     {
//       bt_->Insert(k, le_helper);
//     }
// #else
//     bt_->Insert(k, le_helper);
// #endif
// #endif
  }

  virtual void Delete(const Slice &key) override {
    // TODO
  }

  virtual void Scan(const Slice &key, int cnt,
                    std::vector<ValueType> &vec) override {
    bt_->btree_search_range(*(KeyType *)key.data(), cnt, vec);
  }

  virtual void PrefetchEntry(const Shortcut &sc) override {
    page *p = (page *)sc.GetNodeAddr();
    entry *entry_addr = &p->records[sc.GetPos()];
    __builtin_prefetch(p);
    __builtin_prefetch(entry_addr);
  }

 private:
  btree *bt_;

  DISALLOW_COPY_AND_ASSIGN(FastFairIndex);
};
