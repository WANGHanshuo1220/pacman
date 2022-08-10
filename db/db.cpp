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

#define TIMEDIFF(s, e) (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec) //us

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
#ifdef INTERLEAVED
  // StartRBThread();
#endif
};

DB::~DB() {
#ifdef INTERLEAVED
  // stop_flag_RB.store(true, std::memory_order_release);
  // StopRBThread();
#endif
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

#ifdef INTERLEAVED
void DB::roll_back_()
{
  while (!stop_flag_RB.load(std::memory_order_relaxed))
  {
    if(!is_roll_back_list_empty())
    {
      LogSegment *seg = get_front_roll_back_list();

      uint32_t roll_back_sz = 0;
      bool roll_back = false;
      uint32_t n = seg->num_kvs;

      if(seg->roll_back_map[n-1].first == true)
      {
        for(int i = n - 2; i >= 0; i--)
        {
          roll_back_sz += seg->roll_back_map[i].second;
          roll_back = true;
          seg->num_kvs --;
          seg->roll_back_map.pop_back();
          if(seg->roll_back_map[i].first == true)
          {
            roll_back_sz += seg->roll_back_map[i].second;
            seg->num_kvs --;
            seg->roll_back_map.pop_back();
          }
          else{
            break;
          }
        }
        seg->roll_back_tail(roll_back_sz);
      }

      pop_front_roll_back_list();
    }
  }
  
}
#endif

// class DB::Worker

DB::Worker::Worker(DB *db) : db_(db) {
  worker_id_ = db_->cur_num_workers_.fetch_add(1);
  tmp_cleaner_garbage_bytes_.resize(db_->num_cleaners_, 0);
#if INDEX_TYPE == 3
  reinterpret_cast<MasstreeIndex *>(db_->index_)
      ->MasstreeThreadInit(worker_id_);
#endif
#ifdef INTERLEAVED
  num_hot_segments_ = db->log_->get_num_hot_segments_();
  num_cold_segments_ = db->log_->get_num_cold_segments_();
#endif
}

DB::Worker::~Worker() {
#ifdef GC_EVAL
  printf("\nlatency breakdown: %ld\n", insert_time + update_index_time + check_hotcold_time);
  printf("  check_hotcold_time = %ld\n",check_hotcold_time);
  printf("  insert time = %ld\n", insert_time);
  printf("    change_seg_time = %ld\n",change_seg_time);
  printf("    append_time= %ld\n",append_time);
  long a = 0;
  for(int i = 0; i < db_->get_log_()->get_num_segments_(); i++)
  {
    a += db_->get_log_()->get_segments_(i)->make_new_kv_time;
  }
  printf("      vector_push_back_time= %ld\n", a);
  printf("  update time = %ld\n", update_index_time);
  printf("      Markgarbage_time = %ld\n", MarkGarbage_time);
#endif
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
// #ifdef INTERLEAVED
//   printf("before put:\n");
//   for(int i = 0; i < db_->get_log_()->get_num_cold_segments_(); i++)
//   {
//     LogSegment * l = *db_->get_log_()->get_cold_segment_(i);
//     printf("  %dth segment: tail = %p\n", i, l->get_tail());
//   }
// #endif
#ifdef GC_EVAL
  struct timeval check_hotcold_start;
  gettimeofday(&check_hotcold_start, NULL);
#endif
  bool hot;
#ifdef HOT_COLD_SEPARATE
  hot = db_->hot_key_set_->Exist(key);
#else
  hot = true;
#endif
#ifdef GC_EVAL
  struct timeval check_hotcold_end;
  gettimeofday(&check_hotcold_end, NULL);
  check_hotcold_time += TIMEDIFF(check_hotcold_start, check_hotcold_end);
#endif

#ifdef GC_EVAL
  struct timeval insert_begin;
  gettimeofday(&insert_begin, NULL);
#endif
  ValueType val = MakeKVItem(key, value, hot);
#ifdef GC_EVAL
  struct timeval insert_end;
  gettimeofday(&insert_end, NULL);
  insert_time += TIMEDIFF(insert_begin, insert_end);
#endif

#ifndef LOG_BATCHING
#ifdef GC_EVAL
  struct timeval update_index_begin;
  gettimeofday(&update_index_begin, NULL);
#endif
  UpdateIndex(key, val, hot);
#ifdef GC_EVAL
  struct timeval update_index_end;
  gettimeofday(&update_index_end, NULL);
  update_index_time += TIMEDIFF(update_index_begin, update_index_end);
#endif
#endif

// #ifdef INTERLEAVED
//   for(int i = 0; i < db_->get_log_()->get_num_cold_segments_(); i++)
//   {
//     LogSegment * l = *db_->get_log_()->get_cold_segment_(i);
//     printf("  %dth segment: tail = %p\n", i, l->get_tail());
//   }
//   printf("\n");
// #endif
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
#ifdef GC_EVAL
  struct timeval change_seg_start, change_seg_end, append_start, append_end;
  gettimeofday(&change_seg_start, NULL);
#endif
  ValueType ret = INVALID_VALUE;
  uint64_t i_key = *(uint64_t *)key.data();
  uint32_t epoch = db_->GetKeyEpoch(i_key);
#ifdef INTERLEAVED
  uint32_t sz = sizeof(KVItem) + key.size() + value.size();
  if(hot) accumulative_sz_hot += sz;
  else accumulative_sz_cold += sz;
#endif

#ifdef HOT_COLD_SEPARATE
#ifdef INTERLEAVED
  // printf("cur_cold_sengment_ : %d -> ", cur_cold_segment_);
  LogSegment *&segment = hot ? 
    *(db_->log_->get_hot_segment_(cur_hot_segment_)): 
    *(db_->log_->get_cold_segment_(cur_cold_segment_));
  // cur_hot_segment_ = (cur_hot_segment_ == num_hot_segments_) ?
  //   (cur_hot_segment_ - num_hot_segments_) :
  //   (cur_hot_segment_);
  // cur_cold_segment_ = (cur_cold_segment_ == num_cold_segments_) ?
  //   (cur_cold_segment_ - num_cold_segments_) :
  //   (cur_cold_segment_);
  if(hot)
  {
    if(accumulative_sz_hot > change_seg_threshold)
    {
      accumulative_sz_hot = 0;
      cur_hot_segment_ ++;
    }
    if(cur_hot_segment_ >= num_hot_segments_)
      cur_hot_segment_ -= num_hot_segments_;
  }
  else
  {
    if(accumulative_sz_cold > change_seg_threshold)
    {
      accumulative_sz_cold = 0;
      cur_cold_segment_ ++;
    }
    if(cur_cold_segment_ >= num_cold_segments_) 
      cur_cold_segment_ -= num_cold_segments_;
  }
  // printf("%d\n", cur_cold_segment_);
#else
  LogSegment *&segment = hot ? log_head_ : cold_log_head_;
#endif
#else
  LogSegment *&segment = log_head_;
#endif
#ifdef GC_EVAL
  gettimeofday(&change_seg_end, NULL);
  change_seg_time += TIMEDIFF(change_seg_start, change_seg_end);

  gettimeofday(&append_start, NULL);
#endif
  while (segment == nullptr ||
         (ret = segment->Append(key, value, epoch)) == INVALID_VALUE) {
    FreezeSegment(segment);
    segment = db_->log_->NewSegment(hot);
  }
#ifdef GC_EVAL
  gettimeofday(&append_end, NULL);
  append_time += TIMEDIFF(append_start, append_end);
#endif
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
  KVItem *kv = tp.GetKVItem();
  uint32_t sz;
#ifdef INTERLEAVED
  uint16_t num_ = kv->num;
#endif
#ifdef REDUCE_PM_ACCESS
  sz = tp.size;
  if (sz == 0) {
    ERROR_EXIT("size == 0");
    kv = tp.GetKVItem();
    sz = sizeof(KVItem) + kv->key_size + kv->val_size;
  }
#else
  kv = tp.GetKVItem();
  sz = sizeof(KVItem) + kv->key_size + kv->val_size;
#endif
  int segment_id = db_->log_->GetSegmentID(tp.GetAddr());
  LogSegment *segment = db_->log_->GetSegment(segment_id);
  segment->MarkGarbage(tp.GetAddr(), sz);
#ifdef GC_EVAL
    struct timeval MarkGarbage_start;
    struct timeval MarkGarbage_end;
    gettimeofday(&MarkGarbage_start, NULL);
#endif
#ifdef INTERLEAVED
  segment->roll_back_map[num_].first = true;
  // db_->push_back_roll_back_list(segment);

  bool roll_back = false;
  uint32_t roll_back_sz = 0;
  uint32_t n = segment->num_kvs;
  // printf("roll_back:\n");
  // printf("  seg_start = %p\n", segment->get_segment_start());
  // printf("  old_tail  = %p\n", segment->get_tail());
  // printf("  kv + sz = %p + %d = %p\n", kv, sz, kv + sz);
  // if(segment->get_tail() == (char *)kv + sz)
  // {
  //   segment->roll_back_tail(sz);
  //   // roll_back = true;
  // }
  if(segment->roll_back_map[n-1].first == true)
  {
    roll_back_sz += segment->roll_back_map[n-1].second;
    roll_back = true;
    segment->num_kvs --;
    segment->roll_back_map.pop_back();
    for(int i = n - 2; i >= 0; i--)
    {
      if(segment->roll_back_map[i].first == true)
      {
        roll_back_sz += segment->roll_back_map[i].second;
        segment->num_kvs --;
        segment->roll_back_map.pop_back();
      }
      else{
        break;
      }
    }
    // printf("  new_tail  = %p\n", segment->get_tail());

    segment->roll_back_tail(roll_back_sz);
  }
#else
  // update temp cleaner garbage bytes
  if (db_->num_cleaners_ > 0 ) {
    int cleaner_id = db_->log_->GetSegmentCleanerID(tp.GetAddr());
    tmp_cleaner_garbage_bytes_[cleaner_id] += sz;
  }
#endif
#ifdef GC_EVAL
  gettimeofday(&MarkGarbage_end, NULL);
  MarkGarbage_time += (MarkGarbage_end.tv_sec - MarkGarbage_start.tv_sec) * 1000000 + (MarkGarbage_end.tv_usec - MarkGarbage_start.tv_usec);
#endif
}

void DB::Worker::FreezeSegment(LogSegment *segment) {
  db_->log_->FreezeSegment(segment);
  db_->log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
}
