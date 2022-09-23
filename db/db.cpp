#include "db.h"
#include "db_common.h"
#include "log_structured.h"
#include "hotkeyset.h"
// #include "circlequeue.h"

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
      num_cleaners_(num_class), // change to num_class
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
  next_class_segment_.resize(num_class);
  for(int i = 0; i < num_class; i++)
  {
    next_class_segment_[i].resize(num_workers_);
    for(int j = 0; j < num_workers_; j++)
    {
      next_class_segment_[i][j] = j;
    }
  }
  // init log-structured
  log_ =
      new LogStructured(db_path, log_size, this, num_workers_, num_cleaners_);
};

DB::~DB() {
  uint64_t t = 0;
  for(int i = 0; i < num_class; i++)
  {
    printf("%dth class put_c = %u\n", i, put_c[i].load());
    t += put_c[i].load();
  }
  printf("total puts = %ld\n", t);
#ifdef INTERLEAVED
  for(int i = 0; i < num_class-1; i++)
  {
    printf("hot_set%d size = %ld\n", i+1, hot_key_set_->get_set_sz(i));
  }
  printf("roll_back_times = %ld, bytes = %ld byte (%ldKB, %ldMB)\n", 
    roll_back_count.load(), roll_back_bytes.load(), 
    roll_back_bytes.load()/1024, roll_back_bytes.load()/(1024*1024));
#endif
  delete log_;
  delete index_;
  delete g_index_allocator;
#ifdef INTERLEAVED
  // delete roll_back_queue;
#endif
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

// #ifdef INTERLEAVED
// void DB::roll_back_()
// {
//   LogSegment *segment = nullptr;
//   uint32_t roll_back_sz;
//   uint32_t roll_back_count;
//   uint32_t n;
//   int i;
//   uint8_t status;
//   while (!stop_flag_RB.load(std::memory_order_relaxed))
//   {
//     segment = deque_roll_back_queue();
//     if(segment != nullptr)
//     {
//       {
//         std::lock_guard<std::mutex> lock(segment->seg_lock);
//         if(!segment->is_segment_cleaning() && !segment->is_segment_reserved())
//         {
//           status = segment->get_status();
//           segment->set_RB();
//           roll_back_sz = 0;
//           roll_back_count = 0;
//           n = segment->get_num_kvs();
//           // segment->roll_back_c ++;
//           roll_back_count ++;
//           for(i = n - 1; i >= 0; i--)
//           {
//             if(segment->roll_back_map[i].first == true)
//             {
//               roll_back_sz += segment->roll_back_map[i].second;
//               roll_back_count ++;
//             }
//             else{
//               break;
//             }
//           }

//           roll_back_bytes += roll_back_sz;
//           segment->RB_num_kvs(roll_back_count);
//           segment->roll_back_tail(roll_back_sz);
//           segment->update_Bitmap();
//           segment->set_status(status);
//         }
//       }
//     }
//   }
// }
// #endif

// class DB::Worker

DB::Worker::Worker(DB *db) : db_(db) {
  worker_id_ = db_->cur_num_workers_.fetch_add(1);
  tmp_cleaner_garbage_bytes_.resize(db_->num_cleaners_, 0);
  std::pair<uint32_t, LogSegment **> p;

  log_head_class[0] = db_->log_->NewSegment(0);

  for(int i = 1; i < num_class; i++)
  {
    p = db_->get_class_segment(i, worker_id_);
    log_head_class[i] = *p.second;
    class_seg_working_on[i] = p.first;
  }

  for(int i = 0; i < num_class; i++)
  {
    assert(log_head_class[i]->is_segment_using());
  }

#if INDEX_TYPE == 3
  reinterpret_cast<MasstreeIndex *>(db_->index_)
      ->MasstreeThreadInit(worker_id_);
#endif
}

DB::Worker::~Worker() {
#ifdef GC_EVAL
  long a[5] = {0, 0, 0, 0, 0};
  for(int i = 0; i < db_->get_log_()->get_num_segments_(); i++)
  {
    a[0] += db_->get_log_()->get_segments_(i)->make_new_kv_time;
  }
  printf("\nlatency breakdown  = \t%ld us \t(%.2f s)\n", insert_time + update_index_time + check_hotcold_time,
      ((float)insert_time + update_index_time + check_hotcold_time)/1000000);
  // printf("max_kv_sz = %d\n", max_kv_sz);
  printf("  check_hotcold_time = \t%ld us   \t(%.2f s)\n",check_hotcold_time, (float)check_hotcold_time/1000000);
  printf("  insert time        = \t%ld us   \t(%.2f s)\n", insert_time, (float)insert_time/1000000);
  printf("    change_seg_time  = \t%ld us   \t(%.2f s)\n",change_seg_time, (float)change_seg_time/1000000);
  printf("    append_time      = \t%ld us   \t(%.2f s)\n",append_time, (float)append_time/1000000);
  printf("      make_kv_time   = \t%ld us   \t(%.2f s)\n",a[0], (float)a[0]/1000000);
  // printf("      vector_pb_time = \t%ld us   \t(%ld s)\n", a[0], a[0]/1000000);
  // printf("        b1           = \t%ld us   \t(%ld s)\n", a[1], a[1]/1000000);
  // printf("        b2           = \t%ld us   \t(%ld s)\n", a[2], a[2]/1000000);
  // printf("        b3           = \t%ld us   \t(%ld s)\n", a[3], a[3]/1000000);
  // printf("        b4           = \t%ld us   \t(%ld s)\n", a[4], a[4]/1000000);
  printf("      set_seg_time   = \t%ld us   \t(%.2f s)\n", set_seg_time, (float)set_seg_time/1000000);
  printf("  update time        = \t%ld us   \t(%.2f s)\n", update_index_time, (float)update_index_time/1000000);
  printf("    update idx time  = \t%ld us   \t(%.2f s)\n", update_idx_p1, (float)update_idx_p1/1000000);
  printf("    markgarbage time = \t%ld us   \t(%.2f s)\n",MarkGarbage_time, (float)MarkGarbage_time/1000000);
  // printf("      Markgarbage_p1 = \t%ld us   \t(%ld s)\n", markgarbage_p1, markgarbage_p1/1000000);
  // printf("        get_kv_num   = \t%ld us   \t(%ld s)\n", get_kv_num, get_kv_num/1000000);
  // printf("        get_kv_sz    = \t%ld us   \t(%ld s)\n", get_kv_sz, get_kv_sz/1000000);
  // printf("      Markgarbage_p2 = \t%ld us   \t(%ld s)\n", markgarbage_p2, markgarbage_p2/1000000);
#endif
#ifdef LOG_BATCHING
  BatchIndexInsert(buffer_queue_.size(), true);
#ifdef HOT_COLD_SEPARATE
  BatchIndexInsert(cold_buffer_queue_.size(), false);
#endif
#endif
  db_->log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
  for(int i = 0; i < num_class; i++)
  {
    log_head_class[i]->set_touse();
  }
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
/*
* Put opr has three sub-oprs:
*    1. check the hotness of the key;
*    2. make a new kv item, append it at the end of the segment;
*    3. update index
*/
void DB::Worker::Put(const Slice &key, const Slice &value) {

  // sub-opr 1 : check the hotness of the key;
#ifdef GC_EVAL
  struct timeval check_hotcold_start;
  gettimeofday(&check_hotcold_start, NULL);
#endif
  int class_ = db_->hot_key_set_->Exist(key);
  // int class_ = 0;
  db_->put_c[class_].fetch_add(1);
#ifdef GC_EVAL
  struct timeval check_hotcold_end;
  gettimeofday(&check_hotcold_end, NULL);
  check_hotcold_time += TIMEDIFF(check_hotcold_start, check_hotcold_end);
#endif

  // sub-opr 2 : make a new kv item, append it at the end of the segment;
#ifdef GC_EVAL
  struct timeval insert_begin;
  struct timeval insert_end;
  gettimeofday(&insert_begin, NULL);
#endif
  ValueType val = MakeKVItem(key, value, class_);
#ifdef GC_EVAL
  gettimeofday(&insert_end, NULL);
  insert_time += TIMEDIFF(insert_begin, insert_end);
#endif

  // sub-opr 3 : update index;
#ifndef LOG_BATCHING
#ifdef GC_EVAL
  struct timeval update_index_begin;
  gettimeofday(&update_index_begin, NULL);
#endif
  UpdateIndex(key, val, class_);
#ifdef GC_EVAL
  struct timeval update_index_end;
  gettimeofday(&update_index_end, NULL);
  update_index_time += TIMEDIFF(update_index_begin, update_index_end);
#endif
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
#ifdef GC_EVAL
  struct timeval change_seg_start, change_seg_end, append_start, append_end;
  gettimeofday(&change_seg_start, NULL);
  uint32_t sz_ = sizeof(KVItem) + key.size() + value.size();
  if (sz_ > max_kv_sz) max_kv_sz = sz_;
#endif

  ValueType ret = INVALID_VALUE;
  uint64_t i_key = *(uint64_t *)key.data();
  uint32_t epoch = db_->GetKeyEpoch(i_key);

#ifdef INTERLEAVED
  uint32_t sz = sizeof(KVItem) + key.size() + value.size();
  if(hot) {
    if(hot_batch_persistent)
    {
      // int persist_cnt = log_head_->FlushRemain();
      // BatchIndexInsert(persist_cnt, hot);
      std::pair<int, LogSegment **> hot = db_->get_hot_segment();
      log_head_ = *hot.second;
      hot_seg_working_on = hot.first;
      hot_batch_persistent = false;
    }
  }
  else {
    if(cold_batch_persistent)
    {
      // int persist_cnt = cold_log_head_->FlushRemain();
      // BatchIndexInsert(persist_cnt, hot);
      std::pair<int, LogSegment **> cold = db_->get_cold_segment();
      cold_log_head_ = *cold.second;
      cold_seg_working_on = cold.first;
      cold_batch_persistent = false;
    }
  }
#endif

#ifdef HOT_COLD_SEPARATE
  LogSegment *&segment = hot ? log_head_ : cold_log_head_;
  std::queue<std::pair<KeyType, ValueType>> &queue =
      hot ? buffer_queue_ : cold_buffer_queue_;
#else
  LogSegment *&segment = log_head_;
  std::queue<std::pair<KeyType, ValueType>> &queue = buffer_queue_;
#endif

#ifdef GC_EVAL
  gettimeofday(&change_seg_end, NULL);
  change_seg_time += TIMEDIFF(change_seg_start, change_seg_end);
#endif

#ifdef GC_EVAL
  gettimeofday(&append_start, NULL);
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
#ifdef INTERLEAVED
    if(hot)
    {
      db_->log_->set_hot_segment_(hot_seg_working_on, segment);
    }
    else
    {
      db_->log_->set_cold_segment_(cold_seg_working_on, segment);
    }
#endif
  }
  
#ifdef GC_EVAL
  gettimeofday(&append_end, NULL);
  append_time += TIMEDIFF(append_start, append_end);
#endif

  queue.push({i_key, ret});
  if (persist_cnt > 0) {
#ifdef GC_EVAL
  struct timeval update_index_begin;
  gettimeofday(&update_index_begin, NULL);
#endif
  BatchIndexInsert(persist_cnt, hot);
#ifdef GC_EVAL
  struct timeval update_index_end;
  gettimeofday(&update_index_end, NULL);
  update_index_time += TIMEDIFF(update_index_begin, update_index_end);
#endif
#ifdef INTERLEAVED
    if(hot)
    {
      hot_batch_persistent = true;
    }
    else
    {
      cold_batch_persistent = true;
    }
#endif
  }

  assert(ret);
  return ret;
}
#else
ValueType DB::Worker::MakeKVItem(const Slice &key, const Slice &value,
                                 int class_) {
#ifdef GC_EVAL
  struct timeval change_seg_start, change_seg_end, append_start, append_end;
  gettimeofday(&change_seg_start, NULL);
#endif

  ValueType ret = INVALID_VALUE;
  uint64_t i_key = *(uint64_t *)key.data();
  uint32_t epoch = db_->GetKeyEpoch(i_key);

  uint32_t sz = sizeof(KVItem) + key.size() + value.size();

  if(class_ != 0)
  {
    accumulative_sz_class[class_] += sz;
    if(accumulative_sz_class[class_] > db_->get_threshold(class_))
    {
      log_head_class[class_]->set_touse();
      std::pair<uint32_t, LogSegment **> p = db_->get_class_segment(class_, worker_id_);
      log_head_class[class_] = *p.second;
      class_seg_working_on[class_] = p.first;
      accumulative_sz_class[class_] = sz;
      assert(log_head_class[class_]->is_segment_using());
      uint32_t n = log_head_class[class_]->num_kvs;
      if(n)
      {
        if(log_head_class[class_]->roll_back_map[n-1].is_garbage) 
        {
          Roll_Back2(log_head_class[class_]);
        }
      }
    }
  }
  LogSegment *&segment = log_head_class[class_];

#ifdef GC_EVAL
  gettimeofday(&change_seg_end, NULL);
  change_seg_time += TIMEDIFF(change_seg_start, change_seg_end);
#endif

#ifdef GC_EVAL
  gettimeofday(&append_start, NULL);
#endif
  while ((ret = segment->Append(key, value, epoch)) == INVALID_VALUE) {
    FreezeSegment(segment, class_);
    segment = db_->log_->NewSegment(class_);
#ifdef GC_EVAL
    struct timeval s1, e1;
    gettimeofday(&s1, NULL);
#endif
  if(class_ != 0)
  {
    accumulative_sz_class[class_] = sz;
    db_->log_->set_class_segment_(class_, class_seg_working_on[class_], segment);
  }
  // log_head_class[class_] = segment;
#ifdef GC_EVAL
    gettimeofday(&e1, NULL);
    set_seg_time += TIMEDIFF(s1, e1);
#endif
  }
  assert(ret);
#ifdef GC_EVAL
  gettimeofday(&append_end, NULL);
  append_time += TIMEDIFF(append_start, append_end);
#endif
  return ret;
}
#endif

void DB::Worker::UpdateIndex(const Slice &key, ValueType val, int class_) {
#ifdef GC_EVAL
    struct timeval start, end1, end2;
    gettimeofday(&start, NULL);
#endif
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
    db_->hot_key_set_->Record(key, worker_id_, class_);
    db_->thread_status_.rcu_exit(worker_id_);
#endif
#ifdef GC_EVAL
    gettimeofday(&end1, NULL);
    update_idx_p1 += TIMEDIFF(start, end1);
#endif

    MarkGarbage(le_helper.old_val);
#ifdef GC_EVAL
    gettimeofday(&end2, NULL);
    MarkGarbage_time += TIMEDIFF(end1, end2);
#endif
  }
}

void DB::Worker::MarkGarbage(ValueType tagged_val) {
  TaggedPointer tp(tagged_val);

  int segment_id = db_->log_->GetSegmentID(tp.GetAddr());
  LogSegment *segment = db_->log_->GetSegment(segment_id);
  int class_ = segment->get_class();

  if(class_ == 0)
  {
    uint32_t sz = tp.size_or_num;
    if (sz == 0) {
      ERROR_EXIT("size == 0");
      KVItem *kv = tp.GetKVItem();
      sz = sizeof(KVItem) + kv->key_size + kv->val_size;
    }
    segment->MarkGarbage(tp.GetAddr(), sz);
    tmp_cleaner_garbage_bytes_[class_] += sz;
  }
  else
  {
    uint16_t num_ = tp.size_or_num;
    if(num_ == 0xFFFF)
    {
      ERROR_EXIT("num == 0xFFFF\n");
      num_ = tp.GetKVItem()->num;
    }

    uint32_t sz = segment->roll_back_map[num_].kv_sz * kv_align;
    segment->add_garbage_bytes(sz);
    assert(sz > 0);
    uint32_t n = segment->num_kvs.load();

    if( segment->is_segment_touse() )
    {
      if( n-1 == num_)
      {
        assert(segment->roll_back_map[n-1].is_garbage == 0);
        Roll_Back1(sz, segment);
      }
      else
      {
        segment->roll_back_map[num_].is_garbage = 1;
        if(segment->roll_back_map[n-1].is_garbage == 1)
        {
          if(!segment->RB_flag.test_and_set())
          {
            Roll_Back2(segment);
            segment->RB_flag.clear();
          }
        }
      }
    }
    else
    {
      segment->roll_back_map[num_].is_garbage = 1;
    }
  }
}

void DB::Worker::Roll_Back1(uint32_t sz, LogSegment *segment)
{
  uint32_t n = segment->num_kvs;
  assert(segment->roll_back_map[n-1].is_garbage == 0);
  int roll_back_sz = sz;
  uint32_t RB_count = 1;
  db_->roll_back_count.fetch_add(1);
  for(int i = n - 2; i >= 0; i--)
  {
    if(segment->roll_back_map[i].is_garbage == 1)
    {
      segment->roll_back_map[i].is_garbage = 0;
      roll_back_sz += (segment->roll_back_map[i].kv_sz * kv_align);
      RB_count ++;
    }
    else{
      break;
    }
  }
  db_->roll_back_bytes.fetch_add(roll_back_sz);
  segment->roll_back_tail(roll_back_sz);
  segment->reduce_garbage_bytes(roll_back_sz, worker_id_, segment, 1);
  segment->RB_num_kvs(RB_count);
}

void DB::Worker::Roll_Back2(LogSegment *segment)
{
  uint32_t n = segment->num_kvs.load();
  if(segment->roll_back_map[n-1].is_garbage == 1)
  {
    int roll_back_sz = 0;
    uint32_t RB_count = 0;
    db_->roll_back_count.fetch_add(1);
    for(int i = n - 1; i >= 0; i--)
    {
      if(segment->roll_back_map[i].is_garbage == 1)
      {
        segment->roll_back_map[i].is_garbage = 0;
        roll_back_sz += (segment->roll_back_map[i].kv_sz * kv_align);
        RB_count ++;
      }
      else{
        break;
      }
    }
    db_->roll_back_bytes.fetch_add(roll_back_sz);
    segment->roll_back_tail(roll_back_sz);
    segment->reduce_garbage_bytes(roll_back_sz, worker_id_, segment, 2);
    segment->RB_num_kvs(RB_count);
  }
}

void DB::Worker::FreezeSegment(LogSegment *segment, int class_) {
  db_->log_->FreezeSegment(segment, class_);
  if(class_ == 0) db_->log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
}
