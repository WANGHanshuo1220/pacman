#include "config.h"
#include "log_cleaner.h"
#if INDEX_TYPE == 3
#include "index_masstree.h"
#endif


void LogCleaner::RecoverySegments() {
  char *log_start[num_channel];
  for(int i = 0; i < num_channel; i++)
  {
    log_start[i] = log_->pool_start_[i];
  }

  int num_cleaners = log_->num_cleaners_;
  int num_free_seg = 0;
  std::queue<LogSegment *> tmp_free_queue;
  
  int num_segments[num_class];
  num_segments[0] = log_->num_class_segments_[0];
  for(int i = 1; i < num_class; i++)
  {
    num_segments[i] += num_segments[i-1] + log_->num_class_segments_[i];
  }

  int i = cleaner_id_;
  // for class0
  for (; i < num_segments[0]; i += num_cleaners) {
    LogSegment *seg =
        new LogSegment(log_start[cleaner_id_], SEGMENT_SIZE[0], 0, i, cleaner_id_);
    log_start[cleaner_id_] += SEGMENT_SIZE[0];
#ifdef GC_SHORTCUT
    bool has_shortcut = seg->is_hot_ = !seg->header_->has_shortcut;
    if (has_shortcut) {
      seg->set_has_shortcut(true);
      seg->InitShortcutBuffer();
    }
#endif
    // seg->tail_ = seg->data_start_ + seg->header_->offset;
    seg->tail_ = seg->data_start_ + seg->get_offset();

    if (seg->header_->status == StatusAvailable) {
      tmp_free_queue.push(seg);
      ++num_free_seg;
    } else {
      if (seg->header_->status != StatusClosed) {
        ERROR_EXIT("should not happen");
        char *p = seg->get_data_start();
        char *end = seg->get_end();
        while (p < end) {
          KVItem *kv = reinterpret_cast<KVItem *>(p);
          uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
          if (sz == sizeof(KVItem)) {
            break;
          }
          p += sz;
        }
        seg->tail_ = p;
        seg->Close();
      }
      if (seg->is_hot_) {
        closed_hot_segments_.push_back(seg);
      }
      else
      {
        closed_cold_segments_.push_back({seg, 0.});
      }
    }

    log_->all_segments_[i] = seg;
  }

  {
    std::lock_guard<SpinLock> guard(log_->class_list_lock_[0]);
    int num_free = tmp_free_queue.size();
    while (!tmp_free_queue.empty()) {
      log_->free_segments_class[0].push_back(tmp_free_queue.front());
      tmp_free_queue.pop();
    }
    log_->num_free_list_class[0] += num_free;
  }

  // for class1
  if(cleaner_id_ < 2)
  {
    for(int j = 1; j <= 2; j++)
    {
      for (; i < num_segments[j]; i += 2) {
        LogSegment *seg =
            new LogSegment(log_start[cleaner_id_], SEGMENT_SIZE[j], j, i, cleaner_id_);
        log_start[cleaner_id_] += SEGMENT_SIZE[j];
#ifdef GC_SHORTCUT
        bool has_shortcut = seg->is_hot_ = !seg->header_->has_shortcut;
        if (has_shortcut) {
          seg->set_has_shortcut(true);
          seg->InitShortcutBuffer();
        }
#endif
        // seg->tail_ = seg->data_start_ + seg->header_->offset;
        seg->tail_ = seg->data_start_ + seg->get_offset();

        if (seg->header_->status == StatusAvailable) {
          tmp_free_queue.push(seg);
          ++num_free_seg;
        } else if (seg->header_->status == StatusUsing) {
          log_->class_segments_[j].push_back(seg);
        }
        else {
          if (seg->header_->status != StatusClosed) {
            ERROR_EXIT("should not happen");
            char *p = seg->get_data_start();
            char *end = seg->get_end();
            while (p < end) {
              KVItem *kv = reinterpret_cast<KVItem *>(p);
              uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
              if (sz == sizeof(KVItem)) {
                break;
              }
              p += sz;
            }
            seg->tail_ = p;
            seg->Close();
          }
          closed_segments_.push_back(seg);
        }

        log_->all_segments_[i] = seg;
      }

      {
        std::lock_guard<SpinLock> guard(log_->class_list_lock_[j]);
        int num_free = tmp_free_queue.size();
        while (!tmp_free_queue.empty()) {
          log_->free_segments_class[j].push_back(tmp_free_queue.front());
          tmp_free_queue.pop();
        }
        log_->num_free_list_class[j] += num_free;
      }
    }
  }

  // recovery over
  if (++log_->recovery_counter_ == log_->num_cleaners_) {
    log_->rec_cv_.notify_all();
  }
}


void LogCleaner::RecoveryInfo() {
  int num_cleaners = log_->num_cleaners_;

  int recover_seg_cnt = 0;
  int recover_obj_cnt = 0;
  int num_segments = 0;
  for(int i = 0; i < num_class; i++)
  {
    num_segments += log_->num_class_segments_[i];
  }

  for (int i = cleaner_id_; i < num_segments; i += num_cleaners) {
    LogSegment *seg = log_->all_segments_[i];

    if (seg->header_->status != StatusAvailable) {
      char *p = seg->get_data_start();
      char *tail = seg->get_tail();
      while (p < tail) {
        KVItem *kv = reinterpret_cast<KVItem *>(p);
        uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
        if (sz == sizeof(KVItem)) {
          break;
        }
        ValueType val = 0;
        // ValueType val = TaggedPointer((char *)kv, sz);
        Slice key = kv->GetKey();
        ValueType real_val = db_->index_->Get(key);
        if (val != real_val) {
          // mark this as garbage
          seg->MarkGarbage((char *)kv, sz);
          // update temp cleaner garbage bytes
          if (db_->num_cleaners_ > 0) {
            tmp_cleaner_garbage_bytes_[cleaner_id_] += sz;
          }
        }
        p += sz;
        ++recover_obj_cnt;
      }
      ++recover_seg_cnt;
    }
  }
  LOG("cleaner %d recover info of %d segments %d objects", cleaner_id_,
      recover_seg_cnt, recover_obj_cnt);

  // recovery over
  if (++log_->recovery_counter_ == log_->num_cleaners_) {
    log_->rec_cv_.notify_all();
  }
}


void LogCleaner::RecoveryAll() {
#if INDEX_TYPE == 3
  reinterpret_cast<MasstreeIndex *>(db_->index_)
      ->MasstreeThreadInit(log_->num_workers_ + cleaner_id_);
#endif
  char *log_start[num_channel];
  for(int i = 0; i < num_channel; i++)
  {
    log_start[i] = log_->pool_start_[i];
  }

  int num_cleaners = log_->num_cleaners_;
  std::queue<LogSegment *> tmp_free_queue;
  
  int num_segments[num_class];
  num_segments[0] = log_->num_class_segments_[0];
  for(int i = 1; i < num_class; i++)
  {
    num_segments[i] = num_segments[i-1] + log_->num_class_segments_[i];
  }

  int i = cleaner_id_;
  // for class0
  for (; i < num_segments[0]; i += num_cleaners) {
    LogSegment *seg =
        new LogSegment(log_start[cleaner_id_], SEGMENT_SIZE[0], 0, i, cleaner_id_, false);
    log_start[cleaner_id_] += SEGMENT_SIZE[0];
    seg->InitBitmap();
#ifdef GC_SHORTCUT
    bool has_shortcut = seg->is_hot_ = !seg->header_->has_shortcut;
    if (has_shortcut) {
      seg->set_has_shortcut(true);
      seg->InitShortcutBuffer();
    }
#endif
    // seg->tail_ = seg->data_start_ + seg->header_->offset;
    seg->tail_ = seg->data_start_ + seg->get_offset();

    if (seg->header_->status == StatusAvailable) {
      tmp_free_queue.push(seg);
    } else {
      char *p = seg->get_data_start();
      char *end = seg->get_end();
      while (p < end) {
        KVItem *kv = reinterpret_cast<KVItem *>(p);
        uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
        if (sz == sizeof(KVItem)) {
          break;
        }
        p += sz;
      }
      seg->tail_ = p;
      seg->Close();
      if (seg->is_hot_) {
        closed_hot_segments_.push_back(seg);
      }
      else
      {
        closed_cold_segments_.push_back({seg, 0.});
      }
    }
    log_->all_segments_[i] = seg;
  }

  {
    std::lock_guard<SpinLock> guard(log_->class_list_lock_[0]);
    int num_free = tmp_free_queue.size();
    while (!tmp_free_queue.empty()) {
      log_->free_segments_class[0].push_back(tmp_free_queue.front());
      tmp_free_queue.pop();
    }
    log_->num_free_list_class[0] += num_free;
  }

  // for class1
  if(cleaner_id_ < 2)
  {
    for(int j = 1; j <= 2; j++)
    {
      for (; i < num_segments[j]; i += 2) {
        LogSegment *seg =
            new LogSegment(log_start[cleaner_id_], SEGMENT_SIZE[j], j, i, cleaner_id_, false);
        seg->init_RB_map();
        log_start[cleaner_id_] += SEGMENT_SIZE[j];
#ifdef GC_SHORTCUT
        bool has_shortcut = seg->is_hot_ = !seg->header_->has_shortcut;
        if (has_shortcut) {
          seg->set_has_shortcut(true);
          seg->InitShortcutBuffer();
        }
#endif
        // seg->tail_ = seg->data_start_ + seg->header_->offset;
        seg->tail_ = seg->data_start_ + seg->get_offset();

        if (seg->header_->status == StatusAvailable) {
          tmp_free_queue.push(seg);
        } else if (seg->header_->status == StatusUsing) {
          log_->class_segments_[j].push_back(seg);
        }
        else {
          if (seg->header_->status != StatusClosed) {
            ERROR_EXIT("should not happen");
          }
          char *p = seg->get_data_start();
          char *end = seg->get_end();
          while (p < end) {
            KVItem *kv = reinterpret_cast<KVItem *>(p);
            uint32_t sz = sizeof(KVItem) + kv->key_size + kv->val_size;
            if (sz == sizeof(KVItem)) {
              break;
            }
            p += sz;
          }
          seg->tail_ = p;
          seg->Close();
          closed_segments_.push_back(seg);
        }

        log_->all_segments_[i] = seg;
      }

      {
        std::lock_guard<SpinLock> guard(log_->class_list_lock_[j]);
        int num_free = tmp_free_queue.size();
        while (!tmp_free_queue.empty()) {
          log_->free_segments_class[j].push_back(tmp_free_queue.front());
          tmp_free_queue.pop();
        }
        log_->num_free_list_class[j] += num_free;
      }
    }
  }

  // recovery over
  if (++log_->recovery_counter_ == log_->num_cleaners_) {
    log_->rec_cv_.notify_all();
  }
}
