#include <algorithm>
#include <unistd.h>
#include <libpmem.h>
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
    if (NeedCleaning()) {
      GC_times.fetch_add(1, std::memory_order_relaxed);
      DoMemoryClean();
      Timer timer(clean_time_ns_);
    } else {
      usleep(10);
    }
  }
}

bool LogCleaner::NeedCleaning() {

  uint64_t Free, Available, Total;
  double threshold = (double)log_->clean_threshold_ / 100;

  Free = (uint64_t)log_->num_free_list_class[class_] * SEGMENT_SIZE[class_];
  Available = Free + cleaner_garbage_bytes_.load(std::memory_order_relaxed);
  if(class_ == 0) 
  {
    Total = (log_->num_class_segments_[class_] - 1) * SEGMENT_SIZE[class_];
    threshold = std::min(threshold, (double)Available / Total / 2);
  }
  else
  {
    Total = (log_->num_class_segments_[class_] - log_->class_segments_[class_].size() - 1)
             * SEGMENT_SIZE[class_];
  }
  
  // free < 10% of total storage and cleanr_garbage_btyes > free
  return ((double)Free / Total) < threshold;  
}

void LogCleaner::BatchFlush() {
  uint32_t sz =
      volatile_segment_->get_offset() - reserved_segment_->get_offset();
  char *volatile_start =
      volatile_segment_->get_data_start() + reserved_segment_->get_offset();
  char *reserved_start = reserved_segment_->AllocSpace(sz);

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
void LogCleaner::BatchIndexUpdate() {
  std::vector<char *> flush_addr;
  std::vector<ValueType> new_garbage_addr;
  constexpr size_t batch_size = 32;

  TIMER_START_LOGGING(update_index_time_);
  for (size_t i = 0; i < valid_items_.size(); i += batch_size) {
    size_t begin = i;
    size_t end = std::min(valid_items_.size(), begin + batch_size);

    for (size_t j = begin; j < end; ++j) {
#ifdef PREFETCH_ENTRY
      // prefetch next PM read
      if (j + 1 < valid_items_.size() && !valid_items_[j + 1].shortcut.None()) {
        db_->index_->PrefetchEntry(valid_items_[j + 1].shortcut);
      }
#endif

      ValueType new_val = valid_items_[j].new_val;
      LogEntryHelper le_helper(new_val);
      le_helper.old_val = valid_items_[j].old_val;
      le_helper.shortcut = valid_items_[j].shortcut;
      if (!le_helper.shortcut.None()) {
        COUNTER_ADD_LOGGING(shortcut_cnt_, 1);
      }

      db_->index_->GCMove(valid_items_[j].key, le_helper);
// #ifdef GC_SHORTCUT
//       reserved_segment_->AddShortcut(le_helper.shortcut);
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
#ifndef REDUCE_PM_ACCESS
        TaggedPointer tp(new_val);
        // set size to mark garbage
        // tp.size = valid_items_[j].size;
        new_val = (ValueType)tp;
#endif
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

  int gc_cleaner_id =
      log_->GetSegmentCleanerID(reserved_segment_->get_segment_start());
  for (auto it = new_garbage_addr.begin(); it != new_garbage_addr.end(); it++) {
    TaggedPointer tp(*it);
    // reserved_segment_->MarkGarbage(tp.GetAddr(), tp.size);
    // tmp_cleaner_garbage_bytes_[gc_cleaner_id] += tp.size;
  }
  COUNTER_ADD_LOGGING(garbage_move_count_, new_garbage_addr.size());
  COUNTER_ADD_LOGGING(move_count_, valid_items_.size());
  valid_items_.clear();
}

void LogCleaner::CopyValidItemToBuffer(LogSegment *segment) {
  
  char *p = const_cast<char *>(segment->get_data_start());
  char *tail = segment->get_tail();
#ifdef GC_SHORTCUT
  bool has_shortcut = segment->HasShortcut();
  Shortcut *shortcuts = (Shortcut *)tail;
#endif
  uint64_t num_old = 0;
  // printf("tail = %p\n", tail);
  while (p < tail) {
    Shortcut sc;
#ifdef GC_SHORTCUT
    if (has_shortcut) {
      sc = *shortcuts;
      shortcuts++;
    }
#endif
    KVItem *kv = reinterpret_cast<KVItem *>(p);
    uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
    if (sz == sizeof(KVItem)) {
      break;
    }
    if (!volatile_segment_->HasSpaceFor(sz)) {
      // flush reserved segment
      BatchFlush();
      // update reference
      BatchIndexUpdate();
      // new reserved segment
      reserved_segment_->cur_cnt_ += volatile_segment_->cur_cnt_;
      FreezeReservedAndGetNew();
      volatile_segment_->Clear();
    }
    if (!IsGarbage(kv, num_old)) {
      // copy item to buffer
      char *cur = volatile_segment_->AllocOne(sz);
      memcpy(cur, kv, sz);
      uint64_t offset = cur - volatile_segment_->get_data_start();
      char *new_addr = reserved_segment_->get_data_start() + offset;
      Slice key_slice = ((KVItem *)cur)->GetKey();
      valid_items_.emplace_back(key_slice, TaggedPointer((char *)kv, sz, num_old, class_),
                                TaggedPointer(new_addr, sz, num_new, class_), sz, sc);
      num_new ++;
    }
    p += sz;
#ifdef INTERLEAVED
    num_old ++;
#endif
  }
}

// Batch Compact if defined BATCH_COMPACTION
void LogCleaner::BatchCompactSegment(LogSegment *segment) {
  // printf("in BatchCompaction\n");
#ifdef INTERLEAVED
  if(!segment->is_segment_cleaning()) segment->set_cleaning();
  assert(segment->is_segment_cleaning());
#endif
  // copy to DRAM buffer
  CopyValidItemToBuffer(segment);
  // flush reserved segment and update reference
  BatchFlush();
  BatchIndexUpdate();
  cleaner_garbage_bytes_ -= segment->garbage_bytes_;

  // wait for a grace period
  db_->thread_status_.rcu_barrier();

  // free this segment
  segment->Clear();
  if (backup_segment_ == nullptr) {
    backup_segment_ = segment;
  } else {
    std::lock_guard<SpinLock> guard(log_->class_list_lock_[class_]);
    log_->free_segments_class[class_].push(segment);
    ++log_->num_free_list_class[class_];
  }
  ++clean_seg_count_;
}


// CompactSegment is used if not defined BATCH_COMPACTION
void LogCleaner::CompactSegment(LogSegment *segment) {
  // printf("in compaction, class_ = %d\n", segment->get_class());
  // while(segment->is_segment_RB());
  char *p = segment->get_data_start();
  char *tail = segment->get_tail();
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
    if (sz == sizeof(KVItem)) {
      break;
    }
    // printf("  %uth kv_sz = %u\n", num_old, sz);
    if (!reserved_segment_->HasSpaceFor(sz)) {
      // printf("  no space: tail_ = %p, end_ = %p, sz = %u\n",
        // reserved_segment_->get_tail(), reserved_segment_->get_end(), sz);
      FreezeReservedAndGetNew();
    }
    Shortcut sc;
#ifdef GC_SHORTCUT
    if (has_shortcut) {
      sc = *shortcuts;
      shortcuts++;
    }
#endif
    TIMER_START_LOGGING(check_liveness_time_);
    if(class_ == 0) is_garbage = IsGarbage(kv, num_old);
    else is_garbage = segment->roll_back_map[num_old].first;
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
#ifdef INTERLEAVED
      valid_items_.emplace_back(key_slice, TaggedPointer((char *)kv, sz, num_new++),
                                new_val, sz, sc);
#else
      valid_items_.emplace_back(key_slice, TaggedPointer((char *)kv, sz),
                                new_val, sz, sc);
#endif
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
        MarkGarbage(val);
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
    else
    {
      if(class_ != 0) segment->roll_back_map[num_old].first = false;
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
  BatchIndexUpdate();
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
    // printf("  backup\n");
    backup_segment_ = segment;
    backup_segment_->set_reserved();
  } else {
    // printf("  add to free list\n");
    std::lock_guard<SpinLock> guard(log_->class_list_lock_[class_]);
    log_->free_segments_class[class_].push(segment);
    ++log_->num_free_list_class[class_];
  }
  // printf("compaction done...\n");
}

void LogCleaner::FreezeReservedAndGetNew() {
  assert(backup_segment_);
  if (reserved_segment_) {
#if defined(LOG_BATCHING) && !defined(BATCH_COMPACTION)
    reserved_segment_->FlushRemain();
    BatchIndexUpdate();
#endif
    log_->FreezeSegment(reserved_segment_, class_);
    log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
  }
  reserved_segment_ = backup_segment_;
  reserved_segment_->StartUsing(false);
  reserved_segment_->set_reserved();
  assert(reserved_segment_->is_segment_reserved());
  backup_segment_ = nullptr;
#ifdef INTERLEAVED
  num_new = 0;
#endif
}

void LogCleaner::DoMemoryClean() {
  // printf("in domemoryclean, class_ = %d\n", class_);
  TIMER_START_LOGGING(pick_time_);
  LockUsedList();
  to_compact_segments_.splice(to_compact_segments_.end(),
                              closed_segments_);
  UnlockUsedList();

  LogSegment *segment = nullptr;
  double max_score = 0.;
  double max_garbage_proportion = 0.;
  std::list<LogSegment *>::iterator gc_it = to_compact_segments_.end();
  uint64_t cur_time = NowMicros();
  if(class_ != 3)
  {
    for (auto it = to_compact_segments_.begin();
         it != to_compact_segments_.end(); it++) {
      assert(*it);
      double cur_garbage_proportion = (*it)->GetGarbageProportion();
      double cur_score = 1000. * cur_garbage_proportion /
                         (1 - cur_garbage_proportion) *
                         (cur_time - (*it)->get_close_time());
      if (cur_score > max_score) {
        // to record the segment with the most garbage proportion
        max_score = cur_score;
        max_garbage_proportion = cur_garbage_proportion;
        gc_it = it;
      }
    }
  }
  else
  {
    int i = 0;
    for (auto it = to_compact_segments_.begin();
         i < 50 && it != to_compact_segments_.end(); it++, i++) {
      assert(*it);
      double cur_garbage_proportion = (*it)->GetGarbageProportion();
      double cur_score = 1000. * cur_garbage_proportion /
                         (1 - cur_garbage_proportion) *
                         (cur_time - (*it)->get_close_time());
      if (cur_score > max_score) {
        // to record the segment with the most garbage proportion
        max_score = cur_score;
        max_garbage_proportion = cur_garbage_proportion;
        gc_it = it;
      }
    }
  }

  if(gc_it != to_compact_segments_.end()) {
    segment = *gc_it;
    to_compact_segments_.erase(gc_it);
  } else {
    return;
  }

  // update statistics
  // TIMER_STOP_LOGGING(pick_time_);

  // COUNTER_ADD_LOGGING(clean_garbage_bytes_,
  //                     segment->garbage_bytes_.load(std::memory_order_relaxed));
  // COUNTER_ADD_LOGGING(clean_total_bytes_, segment->get_offset());

  // COUNTER_ADD_LOGGING(clean_hot_count_, 1);
  // COUNTER_ADD_LOGGING(hot_clean_garbage_bytes_,
  //                     segment->garbage_bytes_.load(std::memory_order_relaxed));
  // COUNTER_ADD_LOGGING(hot_clean_total_bytes_, segment->get_offset());

#ifdef BATCH_COMPACTION
  BatchCompactSegment(segment);
#else
  CompactSegment(segment);
#endif
}

void LogCleaner::MarkGarbage(ValueType tagged_val) {
  TaggedPointer tp(tagged_val);

  int class_ = reserved_segment_->get_class();

  if(class_ == 0)
  {
    uint32_t sz = tp.size_or_num;
    if (sz == 0) {
      ERROR_EXIT("size == 0");
      KVItem *kv = tp.GetKVItem();
      sz = sizeof(KVItem) + kv->key_size + kv->val_size;
    }
    reserved_segment_->MarkGarbage(tp.GetAddr(), sz);
    tmp_cleaner_garbage_bytes_[class_] += sz;
  }
  else
  {
    uint16_t num_ = tp.size_or_num;
    if(num_ == 0xFFFF)
    {
      printf("num == 0xFFFF\n");
      num_ = tp.GetKVItem()->num;
    }

    reserved_segment_->roll_back_map[num_].first = true;
    // printf("from LogCleaner::MarkGarbage ");
    reserved_segment_->add_garbage_bytes(reserved_segment_->roll_back_map[num_].second);
  }
}
