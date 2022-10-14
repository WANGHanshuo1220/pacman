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
  uint64_t c = 0;
  for(int i = 0; i < num_class; i++)
  {
    c += put_c[i].load();
  }
  printf("total puts = %ld\n", c);
  printf("total gets = %ld\n", get_c.load());
  printf("total oprs = %ld\n", get_c.load() + c);
  printf("RB_c = %ld, RB_bytes = %ld KB (%ld MB)\n",
    roll_back_count.load(), roll_back_bytes.load()/1024,
    roll_back_bytes.load()/(1024*1024));

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

DB::Worker::Worker(DB *db) : db_(db) {
  worker_id_ = db_->cur_num_workers_.fetch_add(1);
  tmp_cleaner_garbage_bytes_.resize(db_->num_cleaners_, 0);
  std::pair<uint32_t, LogSegment **> p;

  log_head_class[0] = db_->log_->NewSegment(0);

  for(int i = 1; i < num_class; i++)
  {
    db_->get_class_segment(i, worker_id_,
                           &log_head_class[i], &class_seg_working_on[i]);
  }

#ifdef LOG_BATCHING
  buffer_queue_.resize(num_class);
#endif

#if INDEX_TYPE == 3
  reinterpret_cast<MasstreeIndex *>(db_->index_)
      ->MasstreeThreadInit(worker_id_);
#endif
}

DB::Worker::~Worker() {
#ifdef LOG_BATCHING
  for(int i = 0; i < num_class; i++)
  {
    BatchIndexInsert(buffer_queue_[i].size(), i);
  }
#endif
  db_->log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
  for(int i = 0; i < num_class; i++)
  {
    log_head_class[i]->set_touse();
  }
  db_->cur_num_workers_--;
}

bool DB::Worker::Get(const Slice &key, std::string *value) {
  // db_->get_c.fetch_add(1);
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
  int class_ = db_->hot_key_set_->Exist(key);
  // db_->put_c[class_].fetch_add(1);

  // sub-opr 2 : make a new kv item, append it at the end of the segment;
  ValueType val = MakeKVItem(key, value, class_);

  // sub-opr 3 : update index;
#ifndef LOG_BATCHING
  UpdateIndex(key, val, class_);
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
void DB::Worker::BatchIndexInsert(int cnt, int class_) {
  std::queue<std::pair<KeyType, ValueType>> &queue =
    buffer_queue_[class_];
  while (cnt--) {
    std::pair<KeyType, ValueType> kv_pair = queue.front();
    UpdateIndex(Slice((const char *)&kv_pair.first, sizeof(KeyType)),
                kv_pair.second, class_);
    queue.pop();
  }
}

ValueType DB::Worker::MakeKVItem(const Slice &key, const Slice &value,
                                 int class_) {
  ValueType ret = INVALID_VALUE;
  uint64_t i_key = *(uint64_t *)key.data();
  uint32_t epoch = db_->GetKeyEpoch(i_key);
  uint32_t sz = sizeof(KVItem) + key.size() + value.size();
  int persist_cnt = 0;

  std::queue<std::pair<KeyType, ValueType>> &queue =
    buffer_queue_[class_];

  if(class_ != 0)
  {
    accumulative_sz_class[class_] += sz;
    if(accumulative_sz_class[class_] > db_->get_threshold(class_))
    {
      persist_cnt = log_head_class[class_]->FlushRemain();
      BatchIndexInsert(persist_cnt, class_);
      assert(queue.size() == 0);

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

  persist_cnt = 0;
  while (segment == nullptr ||
         (ret = segment->AppendBatchFlush(key, value, epoch, &persist_cnt)) ==
             INVALID_VALUE) {
    if (segment) {
      persist_cnt = segment->FlushRemain();
      BatchIndexInsert(persist_cnt, class_);
      assert(queue.size() == 0);
      FreezeSegment(segment, class_);
    }
    segment = db_->log_->NewSegment(class_);
    if(class_ != 0)
    {
      accumulative_sz_class[class_] = sz;
      db_->log_->set_class_segment_(class_, class_seg_working_on[class_], segment);
    }
  }

  queue.push({i_key, ret});
  if (persist_cnt > 0) {
  BatchIndexInsert(persist_cnt, class_);
  }

  assert(ret);
  return ret;
}
#else
ValueType DB::Worker::MakeKVItem(const Slice &key, const Slice &value,
                                 int class_) {
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
      // std::pair<uint32_t, LogSegment **> p = db_->get_class_segment(class_, worker_id_);
      // log_head_class[class_] = *p.second;
      // class_seg_working_on[class_] = p.first;
      db_->get_class_segment(class_, worker_id_, 
                             &log_head_class[class_], &class_seg_working_on[class_]);
      accumulative_sz_class[class_] = sz;
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

  while (segment == nullptr
    || (ret = segment->Append(key, value, epoch)) == INVALID_VALUE) {
    FreezeSegment(segment, class_);
    segment = db_->log_->NewSegment(class_);
    if(class_ != 0)
    {
      accumulative_sz_class[class_] = sz;
      db_->log_->set_class_segment_(class_, class_seg_working_on[class_], segment);
    }
  }
  assert(ret);
  return ret;
}
#endif

void DB::Worker::UpdateIndex(const Slice &key, ValueType val, int class_) {
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

    MarkGarbage(le_helper.old_val);
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
    uint32_t n = segment->num_kvs.load();

    if( segment->is_segment_touse() )
    {
      if( n-1 == num_)
      {
        Roll_Back1(n, sz, segment);
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

void DB::Worker::Roll_Back1(uint32_t n_, uint32_t sz, LogSegment *segment)
{
  uint32_t n = segment->num_kvs;
  int roll_back_sz = sz;
  uint32_t RB_count = 1;
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
  // db_->roll_back_count.fetch_add(1);
  // db_->roll_back_bytes.fetch_add(roll_back_sz);
  segment->roll_back_tail(roll_back_sz);
  segment->reduce_garbage_bytes(roll_back_sz);
  segment->RB_num_kvs(RB_count);
}

void DB::Worker::Roll_Back2(LogSegment *segment)
{
  uint32_t n = segment->num_kvs;
  if(segment->roll_back_map[n-1].is_garbage == 1)
  {
    int roll_back_sz = 0;
    uint32_t RB_count = 0;
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
    // db_->roll_back_count.fetch_add(1);
    // db_->roll_back_bytes.fetch_add(roll_back_sz);
    segment->roll_back_tail(roll_back_sz);
    segment->reduce_garbage_bytes(roll_back_sz);
    segment->RB_num_kvs(RB_count);
  }
}

void DB::Worker::FreezeSegment(LogSegment *segment, int class_) {
  db_->log_->FreezeSegment(segment, class_);
  if(class_ == 0) db_->log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
}
