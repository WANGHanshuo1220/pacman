#include <algorithm>
#include <unistd.h>
#include <libpmem.h>
#include <set>
#include "config.h"
#include "log_cleaner.h"
#if INDEX_TYPE == 3
#include "index_masstree.h"
#endif

#ifdef GC_EVAL
  #include <sys/time.h>
  #define TIMEDIFF(s, e) (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec) //us
  #define TEST_CPU_TIME 1 // 1 for cpu time, 0 for real time
#endif

void LogCleaner::CleanerEntry() {
  // bind_core_on_numa(log_->num_workers_ + cleaner_id_);
#if INDEX_TYPE == 3
  reinterpret_cast<MasstreeIndex *>(db_->index_)
      ->MasstreeThreadInit(log_->num_workers_ + cleaner_id_);
#endif

  while (!log_->stop_flag_.load(std::memory_order_relaxed)) {
    if (NeedCleaning(false)) {
      // Timer timer(clean_time_ns_);
      GC_times ++;
      DoMemoryClean(false);
    }
    else if(class_ != 0 && NeedCleaning(true))
    {
      GC_times_help ++;
      DoMemoryClean(true);
    }
    else
    {
      usleep(10);
    }
  }
}

bool LogCleaner::NeedCleaning(bool help) {

  uint64_t Free, Available, Total;
  double threshold;

  if(class_ == 0 || help) 
  { 
    threshold = (double)log_->clean_threshold_[0] / 100;

    Free = (uint64_t)log_->num_free_list_class[0].load(std::memory_order_relaxed)
           * SEGMENT_SIZE[0];
    int num_cleaners = log_->num_cleaners_;
    Free = Free / num_cleaners;
    Available = Free + cleaner_garbage_bytes_.load(std::memory_order_relaxed);
    Total = (log_->num_class_segments_[0] - num_cleaners) 
            / num_cleaners * SEGMENT_SIZE[0];
    threshold = std::min(threshold, (double)Available / Total / 2);
  }
  else
  {
    threshold = (double)log_->clean_threshold_[class_] / 100;
    Free = (uint64_t)log_->num_free_list_class[class_].load(std::memory_order_relaxed)
           * SEGMENT_SIZE[class_];
    Total = (log_->num_class_segments_[class_] - log_->class_segments_[class_].size() - 1)
             * SEGMENT_SIZE[class_];
  }
  
  // free < 10% of total storage and cleanr_garbage_btyes > free
  return ((double)Free / Total) < threshold;  
}

void LogCleaner::BatchFlush(bool help) {
  VirtualSegment *&volatile_segment = 
    help ? volatile_helper_segment_ : volatile_segment_;
  LogSegment *&reserved_segment = 
    help ? reserved_helper_segment_ : reserved_segment_;
  uint32_t sz =
      volatile_segment->get_offset() - reserved_segment->get_offset();
  char *volatile_start =
      volatile_segment->get_data_start() + reserved_segment->get_offset();
  char *reserved_start = reserved_segment->AllocSpace(sz);

  int new_segment_id = log_->GetSegmentID(reserved_start);
  TIMER_START_LOGGING(copy_time_);
#ifdef LOG_PERSISTENT
  // memmove_movnt_avx512f_clflushopt(reserved_start, volatile_start, sz);
  pmem_memcpy_persist(reserved_start, volatile_start, sz);
#else
  memcpy(reserved_start, volatile_start, sz);
#endif
  TIMER_STOP_LOGGING(copy_time_);
  COUNTER_ADD_LOGGING(flush_pass_, 1);
}

// use array (valid_items_) to update index in batch
void LogCleaner::BatchIndexUpdate(bool help) {
  std::vector<char *> flush_addr;
  std::vector<ValueType> new_garbage_addr;
  constexpr size_t batch_size = 32;
  std::vector<ValidItem> &valid_items = 
    help ? valid_helper_items_ : valid_items_;
  LogSegment *&reserved_segment = 
    help ? reserved_helper_segment_ : reserved_segment_;

  TIMER_START_LOGGING(update_index_time_);
  for (size_t i = 0; i < valid_items.size(); i += batch_size) {
    size_t begin = i;
    size_t end = std::min(valid_items.size(), begin + batch_size);

    for (size_t j = begin; j < end; ++j) {
#ifdef PREFETCH_ENTRY
      // prefetch next PM read
      if (j + 1 < valid_items.size() && !valid_items[j + 1].shortcut.None()) {
        db_->index_->PrefetchEntry(valid_items[j + 1].shortcut);
      }
#endif

      ValueType new_val = valid_items[j].new_val;
      LogEntryHelper le_helper(new_val);
      le_helper.old_val = valid_items[j].old_val;
      le_helper.shortcut = valid_items[j].shortcut;
#ifdef HOT_SC
      if(class_ > 0 && !help) le_helper.is_hot_sc = true;
#endif
      if (!le_helper.shortcut.None()) {
        COUNTER_ADD_LOGGING(shortcut_cnt_, 1);
      }

      db_->index_->GCMove(valid_items[j].key, le_helper);
// #ifdef GC_SHORTCUT
//       reserved_segment->AddShortcut(le_helper.shortcut);
// #endif

      if (le_helper.old_val != new_val) {
        // move succeed
#if defined(BATCH_FLUSH_INDEX_ENTRY) && defined(IDX_PERSISTENT)
        // if (le_helper.index_entry == nullptr) {
        //   ERROR_EXIT("index_entry is null, is this a bug in FastFair ?");
        // }
        if (le_helper.index_entry != nullptr) {
          flush_addr.push_back(le_helper.index_entry);
        }
#endif
      } else {
        new_garbage_addr.push_back(new_val);
      }
      COUNTER_ADD_LOGGING(fast_path_, le_helper.fast_path);
    }
#if defined(BATCH_FLUSH_INDEX_ENTRY) && defined(IDX_PERSISTENT)
    for (auto it = flush_addr.begin(); it != flush_addr.end(); it++) {
      pmem_clflushopt(*it);
    }
    sfence();
    flush_addr.clear();
#endif
  }
  TIMER_STOP_LOGGING(update_index_time_);

  for (auto it = new_garbage_addr.begin(); it != new_garbage_addr.end(); it++) {
    TaggedPointer tp(*it);
    if(class_ == 0 || help)
    {
      reserved_segment->MarkGarbage(tp.GetAddr(), tp.size_or_num);
      int gc_cleaner_id = 
        log_->GetSegmentCleanerID(reserved_segment->get_segment_start());
      tmp_cleaner_garbage_bytes_[gc_cleaner_id] += tp.size_or_num;
    }
    else
    {
      reserved_segment->roll_back_map[tp.size_or_num].is_garbage = 1;
      uint32_t gb_b = reserved_segment->roll_back_map[tp.size_or_num].kv_sz * kv_align;
      reserved_segment->add_garbage_bytes(gb_b);
    }
  }
  COUNTER_ADD_LOGGING(garbage_move_count_, new_garbage_addr.size());
  COUNTER_ADD_LOGGING(move_count_, valid_items.size());
  valid_items.clear();
}

void LogCleaner::CopyValidItemToBuffer123(LogSegment *segment) {
  int tier = (segment->get_channel() < num_channel/2) ? 0 : 1;
  char *p = const_cast<char *>(segment->get_data_start());
  char *tail = segment->get_tail();
#ifdef GC_SHORTCUT
  bool has_shortcut = segment->HasShortcut();
  Shortcut *shortcuts = (Shortcut *)tail;
#endif
  uint64_t num_old = 0;
  bool is_garbage = false;
  while (p < tail) {
    Shortcut sc;
#ifdef GC_SHORTCUT
    if (has_shortcut) {
      sc = *shortcuts;
      shortcuts++;
    }
#endif
    KVItem *kv = reinterpret_cast<KVItem *>(p);
    uint32_t sz = sizeof(KVItem) + (kv->key_size + kv->val_size) * kv_align;
    read_times[tier] ++;
    if (!volatile_segment_->HasSpaceFor(sz)) {
      // flush reserved segment
      BatchFlush(false);
      // update reference
      BatchIndexUpdate(false);
      // new reserved segment
      reserved_segment_->cur_cnt_ += volatile_segment_->cur_cnt_;
      FreezeReservedAndGetNew(false);
      volatile_segment_->Clear();
      num_new = 0;
    }
    is_garbage = segment->roll_back_map[num_old].is_garbage;
    if (!is_garbage) {
      // copy item to buffer
      char *cur = volatile_segment_->AllocOne(sz);
      memcpy(cur, kv, sz);
      flush_times[tier] ++;
      uint64_t offset = cur - volatile_segment_->get_data_start();
      char *new_addr = reserved_segment_->get_data_start() + offset;
      Slice key_slice = ((KVItem *)cur)->GetKey();
      valid_items_.emplace_back(key_slice, TaggedPointer((char *)kv, sz, num_old, class_),
                                TaggedPointer(new_addr, sz, num_new, class_), sz, sc);
      reserved_segment_->roll_back_map[num_new].kv_sz = (uint16_t)(sz/kv_align);
      num_new ++;
    }
    p += sz;
    num_old ++;
  }
}

void LogCleaner::CopyValidItemToBuffer0(LogSegment *segment, bool help) {
  int tier = (segment->get_channel() < num_channel/2) ? 0 : 1;
  char *p = const_cast<char *>(segment->get_data_start());
  char *tail = segment->get_tail();
  VirtualSegment *&volatile_segment = 
    help ? volatile_helper_segment_ : volatile_segment_;
  std::vector<ValidItem> &valid_items = 
    help ? valid_helper_items_ : valid_items_;
  LogSegment *&reserved_segment = 
    help ? reserved_helper_segment_ : reserved_segment_;
#ifdef GC_SHORTCUT
  bool has_shortcut = segment->HasShortcut();
  Shortcut *shortcuts = (Shortcut *)tail;
#endif
  while (p < tail) {
    Shortcut sc;
#ifdef GC_SHORTCUT
    if (has_shortcut) {
      sc = *shortcuts;
      shortcuts++;
    }
#endif
    KVItem *kv = reinterpret_cast<KVItem *>(p);
    uint32_t sz = sizeof(KVItem) + (kv->key_size + kv->val_size) * kv_align;
    read_times[tier] ++;
    if (!volatile_segment->HasSpaceFor(sz)) {
      // flush reserved segment
      BatchFlush(help);
      // update reference
      BatchIndexUpdate(help);
      // new reserved segment
      reserved_segment->cur_cnt_ += volatile_segment->cur_cnt_;
      FreezeReservedAndGetNew(help);
      volatile_segment->Clear();
    }
    if (!IsGarbage(kv)) {
      // copy item to buffer
      char *cur = volatile_segment->AllocOne(sz);
      memcpy(cur, kv, sz);
      flush_times[tier] ++;
      uint64_t offset = cur - volatile_segment->get_data_start();
      char *new_addr = reserved_segment->get_data_start() + offset;
      Slice key_slice = ((KVItem *)cur)->GetKey();
      valid_items.emplace_back(key_slice, TaggedPointer((char *)kv, sz, 0, 0),
                                TaggedPointer(new_addr, sz, 0, 0), sz, sc);
    }
    p += sz;
  }
}

// Batch Compact if defined BATCH_COMPACTION
void LogCleaner::BatchCompactSegment(LogSegment *segment, bool help) {
  // copy to DRAM buffer
  if(class_ == 0 || help) CopyValidItemToBuffer0(segment, help);
  else CopyValidItemToBuffer123(segment);
  // flush reserved segment and update reference
  BatchFlush(help);
  BatchIndexUpdate(help);
  cleaner_garbage_bytes_ -= segment->garbage_bytes_;

  // wait for a grace period
  db_->thread_status_.rcu_barrier();

  // free this segment
  segment->Clear();
  // segment->gc_c++;

  LogSegment *&backup_segment = 
    help ? backup_helper_segment_ : backup_segment_;
  int class_t = help ? 0 : class_;
  int channel = segment->get_channel();

  if (backup_segment == nullptr) {
    backup_segment = segment;
  } else {
    std::lock_guard<SpinLock> guard(log_->class_list_lock_[class_t]);
    if(class_t == 0)
    {
      log_->free_segments_class0[channel].push_back(segment);
    }
    else
    {
      log_->free_segments_class[class_t].push_back(segment);
    }
    ++log_->num_free_list_class[class_t];
  }
  ++clean_seg_count_;
}

void LogCleaner::CompactSegment0(LogSegment *segment, bool help) {
  char *p = segment->get_data_start();
  char *tail = segment->get_tail();
  LogSegment *&reserved_segment = 
    help ? reserved_helper_segment_ : reserved_segment_;
  std::vector<char *> flush_addr;
  bool is_garbage;
#ifdef GC_SHORTCUT
  bool has_shortcut = segment->HasShortcut();
  Shortcut *shortcuts = (Shortcut *)tail;
#endif
  uint32_t num_old = 0;
  while (p < tail) {
    KVItem *kv = reinterpret_cast<KVItem *>(p);
    uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
    Shortcut sc;
    if (sz == sizeof(KVItem)) {
      break;
    }
    if (!reserved_segment->HasSpaceFor(sz)) {
      FreezeReservedAndGetNew(help);
    }
#ifdef GC_SHORTCUT
    if (has_shortcut) {
      sc = *shortcuts;
      shortcuts++;
    }
#endif
    TIMER_START_LOGGING(check_liveness_time_);
    is_garbage = IsGarbage(kv);
    TIMER_STOP_LOGGING(check_liveness_time_);
    if (!is_garbage) {
#ifdef LOG_BATCHING
      // batch persist the log as workers, update index in batch
      int persist_cnt = 0;  // ignore, batch update all
      TIMER_START_LOGGING(copy_time_);
      ValueType new_val = reserved_segment->AppendBatchFlush(
          kv->GetKey(), kv->GetValue(), kv->epoch, &persist_cnt);
      TIMER_STOP_LOGGING(copy_time_);
      Slice key_slice = TaggedPointer(new_val).GetKVItem()->GetKey();
      valid_items_.emplace_back(key_slice, TaggedPointer((char *)kv, sz, num_new++, class_),
                                new_val, sz, sc);
#else // not LOG_BATCHING
      Slice key = kv->GetKey();
      Slice data = kv->GetValue();
      TIMER_START_LOGGING(copy_time_);
      ValueType val = reserved_segment->Append(key, data, kv->epoch);
      TIMER_STOP_LOGGING(copy_time_);
      LogEntryHelper le_helper(val);
      le_helper.old_val = TaggedPointer(p, sz, num_old, class_);
      le_helper.shortcut = sc;
      TIMER_START_LOGGING(update_index_time_);
      db_->index_->GCMove(key, le_helper);
      TIMER_STOP_LOGGING(update_index_time_);
// #ifdef GC_SHORTCUT
//       reserved_segment->AddShortcut(le_helper.shortcut);
// #endif
      if (le_helper.old_val == val) {
        MarkGarbage0(val);
        COUNTER_ADD_LOGGING(garbage_move_count_, 1);
      }
#if defined(BATCH_FLUSH_INDEX_ENTRY) && defined(IDX_PERSISTENT)
      else {
        // if (le_helper.index_entry == nullptr) {
        //   ERROR_EXIT("index_entry is null, is this a bug in FastFair ?");
        // }
        if (le_helper.index_entry != nullptr) {
          flush_addr.push_back(le_helper.index_entry);
          if (flush_addr.size() >= 32) {
            for (int i = 0; i < flush_addr.size(); i++) {
              pmem_clflushopt(flush_addr[i]);
            }
            sfence();
            flush_addr.clear();
          }
        }
      }
#endif
      COUNTER_ADD_LOGGING(move_count_, 1);
      COUNTER_ADD_LOGGING(fast_path_, le_helper.fast_path);
#endif  // end of #IF LOG_BATCHING
    }
    p += sz;
#ifdef INTERLEAVED
    num_old ++;
#endif
  }

#ifdef LOG_BATCHING
  TIMER_START_LOGGING(copy_time_);
  reserved_segment->FlushRemain();
  TIMER_STOP_LOGGING(copy_time_);
  BatchIndexUpdate(help);
#else
#if defined(BATCH_FLUSH_INDEX_ENTRY) && defined(IDX_PERSISTENT)
  for (int i = 0; i < flush_addr.size(); i++) {
    pmem_clflushopt(flush_addr[i]);
  }
  sfence();
  flush_addr.clear();
#endif
#endif
  // wait for a grace period
  db_->thread_status_.rcu_barrier();

  ++clean_seg_count_;
  segment->Clear();

  LogSegment *&backup_segment = 
    help ? backup_helper_segment_ : backup_segment_;
  int class_t = help ? 0 : class_;

  if (backup_segment == nullptr) {
    backup_segment = segment;
  } else {
    std::lock_guard<SpinLock> guard(log_->class_list_lock_[class_t]);
    log_->free_segments_class[class_t].push_back(segment);
    ++log_->num_free_list_class[class_t];
  }
}

// CompactSegment is used if not defined BATCH_COMPACTION
void LogCleaner::CompactSegment123(LogSegment *segment) {
  char *p = segment->get_data_start();
  char *tail = segment->get_tail();
  std::vector<char *> flush_addr;
  uint16_t is_garbage;
#ifdef GC_SHORTCUT
  bool has_shortcut = segment->HasShortcut();
  Shortcut *shortcuts = (Shortcut *)tail;
#endif
  uint32_t num_old = 0;
  while (p < tail) {
    KVItem *kv = reinterpret_cast<KVItem *>(p);
    uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
    Shortcut sc;
    if (sz == sizeof(KVItem)) {
      break;
    }
    if (!reserved_segment_->HasSpaceFor(sz)) {
      FreezeReservedAndGetNew(false);
    }
#ifdef GC_SHORTCUT
    if (has_shortcut) {
      sc = *shortcuts;
      shortcuts++;
    }
#endif
    TIMER_START_LOGGING(check_liveness_time_);
    is_garbage = segment->roll_back_map[num_old].is_garbage;
    TIMER_STOP_LOGGING(check_liveness_time_);
    if (!is_garbage) {
#ifdef LOG_BATCHING
      // batch persist the log as workers, update index in batch
      int persist_cnt = 0;  // ignore, batch update all
      TIMER_START_LOGGING(copy_time_);
      ValueType new_val = reserved_segment_->AppendBatchFlush(
          kv->GetKey(), kv->GetValue(), kv->epoch, &persist_cnt);
      TIMER_STOP_LOGGING(copy_time_);
      Slice key_slice = TaggedPointer(new_val).GetKVItem()->GetKey();
      valid_items_.emplace_back(key_slice, TaggedPointer((char *)kv, sz, num_new++, class_),
                                new_val, sz, sc);
#else // not LOG_BATCHING
      Slice key = kv->GetKey();
      Slice data = kv->GetValue();
      TIMER_START_LOGGING(copy_time_);
      ValueType val = reserved_segment_->Append(key, data, kv->epoch);
      TIMER_STOP_LOGGING(copy_time_);
      LogEntryHelper le_helper(val);
      le_helper.old_val = TaggedPointer(p, sz, num_old, class_);
      le_helper.shortcut = sc;
      TIMER_START_LOGGING(update_index_time_);
      db_->index_->GCMove(key, le_helper);
      TIMER_STOP_LOGGING(update_index_time_);
// #ifdef GC_SHORTCUT
//       reserved_segment_->AddShortcut(le_helper.shortcut);
// #endif
      if (le_helper.old_val == val) {
        MarkGarbage123(val);
        COUNTER_ADD_LOGGING(garbage_move_count_, 1);
      }
#if defined(BATCH_FLUSH_INDEX_ENTRY) && defined(IDX_PERSISTENT)
      else {
        // if (le_helper.index_entry == nullptr) {
        //   ERROR_EXIT("index_entry is null, is this a bug in FastFair ?");
        // }
        if (le_helper.index_entry != nullptr) {
          flush_addr.push_back(le_helper.index_entry);
          if (flush_addr.size() >= 32) {
            for (int i = 0; i < flush_addr.size(); i++) {
              pmem_clflushopt(flush_addr[i]);
            }
            sfence();
            flush_addr.clear();
          }
        }
      }
#endif
      COUNTER_ADD_LOGGING(move_count_, 1);
      COUNTER_ADD_LOGGING(fast_path_, le_helper.fast_path);
#endif  // end of #IF LOG_BATCHING
    }
    p += sz;
#ifdef INTERLEAVED
    num_old ++;
#endif
  }

#ifdef LOG_BATCHING
  TIMER_START_LOGGING(copy_time_);
  reserved_segment_->FlushRemain();
  TIMER_STOP_LOGGING(copy_time_);
  BatchIndexUpdate(help);
#else
#if defined(BATCH_FLUSH_INDEX_ENTRY) && defined(IDX_PERSISTENT)
  for (int i = 0; i < flush_addr.size(); i++) {
    pmem_clflushopt(flush_addr[i]);
  }
  sfence();
  flush_addr.clear();
#endif
#endif
  // wait for a grace period
  db_->thread_status_.rcu_barrier();

  ++clean_seg_count_;
  segment->Clear();
  if (backup_segment_ == nullptr) {
    backup_segment_ = segment;
  } else {
    std::lock_guard<SpinLock> guard(log_->class_list_lock_[class_]);
    log_->free_segments_class[class_].push_back(segment);
    ++log_->num_free_list_class[class_];
  }
}

void LogCleaner::FreezeReservedAndGetNew(bool help) {
  LogSegment *&reserved_segment =
    help ? reserved_helper_segment_ : reserved_segment_;
  LogSegment *&backup_segment = 
    help ? backup_helper_segment_ : backup_segment_;

  assert(backup_segment);
  if (reserved_segment) {
#if defined(LOG_BATCHING) && !defined(BATCH_COMPACTION)
    reserved_segment->FlushRemain();
    BatchIndexUpdate();
#endif
    log_->FreezeSegment(reserved_segment, reserved_segment->get_class());
    log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
  }
  reserved_segment = backup_segment;
  reserved_segment->StartUsing(false);
  backup_segment = nullptr;
}

void LogCleaner::DoMemoryClean(bool help)
{
  static int c = 0;
  LogSegment *segment = nullptr;
  double max_score = 0.;
  double cold_score = 0.;
  double max_garbage_proportion = 0.;

  if(class_ == 0 || help)
  {
    LockUsedList();
    to_compact_hot_segments_.splice(to_compact_hot_segments_.end(),
                                    closed_hot_segments_);
    UnlockUsedList();
    std::list<LogSegment *>::iterator gc_it = to_compact_hot_segments_.end();

    uint64_t cur_time = NowMicros();
    for (auto it = to_compact_hot_segments_.begin();
         it != to_compact_hot_segments_.end(); it++) {
      double cur_garbage_proportion = (*it)->GetGarbageProportion();
      double cur_score = 1000. * cur_garbage_proportion /
                         (1 - cur_garbage_proportion) *
                         (cur_time - (*it)->get_close_time());
      if (cur_score > max_score) {
        max_score = cur_score;
        max_garbage_proportion = cur_garbage_proportion;
        gc_it = it;
      }
    }

    if (max_garbage_proportion < 0.99 && cur_time - last_update_time_ > 1e6) {
    // if (max_garbage_proportion < 0.90) {
      // update cold segment list
      LockUsedList();
      to_compact_cold_segments_.splice(to_compact_cold_segments_.end(),
                                       closed_cold_segments_);
      UnlockUsedList();
      last_update_time_ = cur_time;
      for (auto it = to_compact_cold_segments_.begin();
           it != to_compact_cold_segments_.end(); it++) {
        double garbage_proportion = it->segment->GetGarbageProportion();
        it->compaction_score = 1000. * garbage_proportion /
                               (1 - garbage_proportion) *
                               (cur_time - it->segment->get_close_time());
      }
      if (!to_compact_cold_segments_.empty()) {
        to_compact_cold_segments_.sort();
        cold_score = to_compact_cold_segments_.begin()->compaction_score;
      }
    } else if (!to_compact_cold_segments_.empty()) {
      LogSegment *seg = to_compact_cold_segments_.begin()->segment;
      double garbage_proportion = seg->GetGarbageProportion();
      cold_score = 1000. * garbage_proportion / (1 - garbage_proportion) *
                   (cur_time - seg->get_close_time());
    }

    if (cold_score > max_score) {
      segment = to_compact_cold_segments_.begin()->segment;
      to_compact_cold_segments_.erase(to_compact_cold_segments_.begin());
    } else if (gc_it != to_compact_hot_segments_.end()) {
      segment = *gc_it;
      to_compact_hot_segments_.erase(gc_it);
    } else {
      return;
    }
  }
  else
  {
    LockUsedList();
    to_compact_segments_.splice(to_compact_segments_.end(),
                                    closed_segments_);
    UnlockUsedList();
    std::list<LogSegment *>::iterator gc_it = to_compact_segments_.end();

    for (auto it = to_compact_segments_.begin();
         it != to_compact_segments_.end();) {
      assert(*it);
      double cur_garbage_proportion = (*it)->GetGarbageProportion();
      if(cur_garbage_proportion >= 1)
      {
        quick_c ++;
        segment = *it;
        it = to_compact_segments_.erase(it);
        segment->Clear();

        {
          std::lock_guard<SpinLock> guard(log_->class_list_lock_[class_]);
          log_->free_segments_class[class_].push_back(segment);
          ++log_->num_free_list_class[class_];
        }
      }
      else
      {
        double cur_score = 1000. * cur_garbage_proportion /
                           (1 - cur_garbage_proportion);
        if (cur_score > max_score) {
          max_score = cur_score;
          max_garbage_proportion = cur_garbage_proportion;
          gc_it = it;
        }
        it++;
      }
    }

    if (gc_it != to_compact_segments_.end()) {
      segment = *gc_it;
      to_compact_segments_.erase(gc_it);
    } else {
      return;
    }
  }

#ifdef BATCH_COMPACTION
  BatchCompactSegment(segment, help);
#else
  if(class_ == 0 || help) CompactSegment0(segment, help);
  else CompactSegment123(segment);
#endif
}

void LogCleaner::MarkGarbage0(ValueType tagged_val) {
  TaggedPointer tp(tagged_val);

  int class_ = reserved_segment_->get_class();

  uint32_t sz = tp.size_or_num;
  if (sz == 0) {
    ERROR_EXIT("size == 0");
    KVItem *kv = tp.GetKVItem();
    sz = sizeof(KVItem) + kv->key_size + kv->val_size;
  }
  reserved_segment_->MarkGarbage(tp.GetAddr(), sz);
  tmp_cleaner_garbage_bytes_[class_] += sz;
}

void LogCleaner::MarkGarbage123(ValueType tagged_val) {
  TaggedPointer tp(tagged_val);

  uint16_t num_ = tp.size_or_num;
  reserved_segment_->roll_back_map[num_].is_garbage = 1;
  reserved_segment_->add_garbage_bytes(reserved_segment_->roll_back_map[num_].kv_sz);
}

void LogCleaner::MarkGarbage_recovery(ValueType tagged_val) {
  TaggedPointer tp(tagged_val);

  int segment_id = db_->log_->GetSegmentID(tp.GetAddr());
  LogSegment *segment = db_->log_->GetSegment(segment_id);
  int class_t = segment->get_class();

  if(class_t == 0)
  {
    uint32_t sz = tp.size_or_num;
    if (sz == 0) {
      ERROR_EXIT("size == 0");
      KVItem *kv = tp.GetKVItem();
      sz = sizeof(KVItem) + (kv->key_size + kv->val_size) * kv_align;
    }
    segment->MarkGarbage(tp.GetAddr(), sz);
    int cleaner_id = db_->log_->GetSegmentCleanerID(tp.GetAddr());
    tmp_cleaner_garbage_bytes_[cleaner_id] += sz;
  }
  else
  {
    uint16_t num_ = tp.size_or_num;
    uint32_t sz = segment->roll_back_map[num_].kv_sz * kv_align;
    segment->add_garbage_bytes(sz);
    segment->roll_back_map[num_].is_garbage = 1;
  }
}