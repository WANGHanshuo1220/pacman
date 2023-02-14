#include "db.h"
#include "db_common.h"
#include "log_structured.h"
#include "hotkeyset.h"
#include <random>

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

DB::DB(std::string db_path[], size_t log_size, int num_workers, int num_cleaners)
    : num_workers_(num_workers),
      num_cleaners_(num_cleaners),
#ifdef HOT_SC
      hot_set_status_(num_workers),
#endif
      thread_status_(num_workers) {
#if defined(USE_PMDK) && defined(IDX_PERSISTENT)
  g_index_allocator = new PMDKAllocator(db_path[0] + "/idx_pool", IDX_POOL_SIZE);
#else
  g_index_allocator = new MMAPAllocator(db_path[0] + "/idx_pool", IDX_POOL_SIZE);
#endif

#if INDEX_TYPE <= 1
  index_ = new CCEHIndex(this);
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
  for(int i = 1; i < num_class; i++)
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
  // printf("get = %.2f\n", (double)t_get_t.load()/t_get_c.load());
  // printf("put = %.2f\n", (double)t_put_t.load()/t_put_c.load());
  
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
#ifdef HOT_SC
  if(has_hot_set())
  {
    uint64_t t = 0;
    for(auto it = hot_sc->begin();
        it != hot_sc->end(); it++)
    {
      t += it->second->lock.get_contendedTime();
      delete it->second;
    }
    delete hot_sc;
    printf("total contendedTime = %ld\n", t);
  }
#endif
}

#ifdef HOT_SC
bool DB::has_hot_set()
{
  if(hot_key_set_ == nullptr) return false;
  else return hot_key_set_->has_hot_set();
}

bool DB::not_changing()
{
  return hot_key_set_->not_changing();
}

bool DB::is_changing()
{
  return hot_key_set_->is_changing();
}

bool DB::has_key_in_sc(KeyType key, std::string *value)
{
  bool ret = false;
  if(has_hot_set() && not_changing())
  {
    if(hot_sc->find(key) != hot_sc->end())
    {
      if((*hot_sc)[key]->value != "default")
      {
        *value = (*hot_sc)[key]->value;
        ret = true;
      }
    }
  }
  return ret;
}

void DB::GetValue(KeyType key, std::string *value)
{
  *value = (*hot_sc)[key]->value;
}

void DB::update_hot_sc(const Slice &key, LogEntryHelper &le_helper,
                       const Slice &value)
{
  KeyType k = *(KeyType *)key.data();
  ValueType tagged_addr = le_helper.new_val;
  ValueType old_val = 0;
  {
    std::lock_guard<SpinLock> lock((*hot_sc)[k]->lock);
    if((*hot_sc)[k]->first == true)
    {
      (*hot_sc)[k]->first = false;
      old_val = index_->Get(key);
    }
    else
    {
      old_val = (*hot_sc)[k]->addr;
    }
    (*hot_sc)[k]->addr   = tagged_addr;
    (*hot_sc)[k]->value  = value.data();
  }
  le_helper.old_val = old_val;
}

bool DB::mark_invalide(const Slice key, LogEntryHelper &le_helper)
{
  bool ret = true;
  KeyType k = *(KeyType *)key.data();
  if(hot_sc->find(k) != hot_sc->end())
  {
    if((*hot_sc)[k]->lock.try_lock())
    {
      if((*hot_sc)[k]->valide == true)
      {
        (*hot_sc)[k]->valide = false;
        if((*hot_sc)[k]->addr != INVALID_VALUE) 
          le_helper.old_val = (*hot_sc)[k]->addr;
      }
    }
  }
  else
  {
    ret = false;
  }
  return ret;
}

void DB::check_val_addr(const Slice key, ValueType addr)
{
  TaggedPointer tp(addr);
  KeyType old_key = *(KeyType *)(tp.GetKVItem()->GetKey().data());
  KeyType key_ = *(KeyType *)(key.data());
  if(key_ != old_key)
  {
    TaggedPointer tp(addr);
    int segment_id = log_->GetSegmentID(tp.GetAddr());
    LogSegment *segment = log_->GetSegment(segment_id);
    printf("seg.st = %d\n", segment->get_status());
    printf("tp.key = %ld\n", old_key);
    printf("key    = %ld\n", key_);
  }
  assert(old_key == key_);
}
#endif

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
  index_ = new CCEHIndex(this);
#elif INDEX_TYPE == 2
  index_ = new FastFairIndex();
#elif INDEX_TYPE == 3
  index_ = new MasstreeIndex();
#endif
}

DB::Worker::Worker(DB *db) : db_(db) {
  worker_id_ = db_->cur_num_workers_.fetch_add(1);
  tmp_cleaner_garbage_bytes_.resize(db_->num_cleaners_, 0);

  log_head_class[0] = db_->log_->NewSegment(0);
  log_head_cold_class0_ = db_->log_->NewSegment(0);

  for(int i = 1; i < num_class; i++)
  {
    db_->get_class_segment(i, worker_id_,
                           &log_head_class[i], &class_seg_working_on[i]);
  }

#ifdef LOG_BATCHING
  buffer_queue_.resize(num_class+1);
#endif

#if INDEX_TYPE == 3
  reinterpret_cast<MasstreeIndex *>(db_->index_)
      ->MasstreeThreadInit(worker_id_);
#endif
}

DB::Worker::~Worker() {
  // db_->t_get_t += get_t;
  // db_->t_put_t += put_t;
  // db_->t_get_c += get_c;
  // db_->t_put_c += put_c;
  // db_->t_put_c[0] += put_c[0];
  // db_->t_put_c[1] += put_c[1];
  // db_->t_put_c[2] += put_c[2];
  // db_->t_put_c[3] += put_c[3];
  // db_->MKI_T += MKI_t;
  // db_->UDI_T += UDI_t;
  // db_->PID_T += PID_t;
  // db_->MKG_T += MKG_t;
#ifdef LOG_BATCHING
  for(int i = 0; i < num_class + 1; i++)
  {
    BatchIndexInsert(buffer_queue_[i].size(), i-1);
  }
#endif
  db_->log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
  if(log_head_class[0]) 
  {
    FreezeSegment(log_head_class[0], 0);
    log_head_class[0] = nullptr;
  }
  if(log_head_cold_class0_) 
  {
    FreezeSegment(log_head_cold_class0_, -1);
    log_head_cold_class0_ = nullptr;
  }
  db_->cur_num_workers_--;
}

bool DB::Worker::Get(const Slice &key, std::string *value) {
  bool ret = false;
  ValueType val;
#ifdef HOT_SC
  KeyType k = *(KeyType *)key.data();
  db_->hot_set_status_.rcu_progress(worker_id_);
  ret = db_->has_key_in_sc(k, value);
  db_->hot_set_status_.rcu_progress(worker_id_);
  if(ret)
  {
    return ret;
  }
  else
  {
    db_->thread_status_.rcu_progress(worker_id_);
    val = db_->index_->Get(key);
    if (val != INVALID_VALUE) {
      TaggedPointer(val).GetKVItem()->GetValue(*value);
      ret = true;
    }
    db_->thread_status_.rcu_exit(worker_id_);
  }
#else
  db_->thread_status_.rcu_progress(worker_id_);
  val = db_->index_->Get(key);
  if (val != INVALID_VALUE) {
    TaggedPointer(val).GetKVItem()->GetValue(*value);
    ret = true;
  }
  db_->thread_status_.rcu_exit(worker_id_);
#endif
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
  int class_t = db_->hot_key_set_->Exist(key);

  // sub-opr 2 : make a new kv item, append it at the end of the segment;
  ValueType tagged_addr = MakeKVItem(key, value, class_t);

  // sub-opr 3 : update index;
#ifndef LOG_BATCHING
  UpdateIndex(key, tagged_addr, value, class_t);
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
  std::queue<std::pair<std::pair<KeyType, ValueType>, const char*>> &queue =
    buffer_queue_[class_+1];
  while (cnt--) {
    std::pair<std::pair<KeyType, ValueType>, const char*>kv_pair = queue.front();
    UpdateIndex(Slice((const char *)&kv_pair.first.first, sizeof(KeyType)),
                kv_pair.first.second, Slice(kv_pair.second), class_);
    queue.pop();
  }
}

ValueType DB::Worker::MakeKVItem(const Slice &key, const Slice &value,
                                 int class_t) {
  ValueType ret = INVALID_VALUE;
  uint64_t i_key = *(uint64_t *)key.data();
  uint32_t epoch = db_->GetKeyEpoch(i_key);
  uint32_t sz = sizeof(KVItem) + key.size() + value.size();
  int persist_cnt = 0;
  LogSegment *segment = nullptr;

  std::queue<std::pair<std::pair<KeyType, ValueType>, const char*>> &queue =
    buffer_queue_[class_t+1];

  if(class_t == 0)
  {
    segment = log_head_class[0];
  }
  else if(class_t == -1)
  {
    segment = log_head_cold_class0_;
  }
  else
  {
    accumulative_sz_class[class_t] += sz;
    if(accumulative_sz_class[class_t] > db_->get_threshold(class_t))
    {
      // log_head_class[class_t]->Flush_Header();
      persist_cnt = log_head_class[class_t]->FlushRemain();
      if (persist_cnt > 0) {
        BatchIndexInsert(persist_cnt, class_t);
      }
      db_->get_class_segment(class_t, worker_id_, 
                             &log_head_class[class_t], &class_seg_working_on[class_t]);
      accumulative_sz_class[class_t] = sz;
      int n = log_head_class[class_t]->num_kvs;
      if(n)
      {
        if(log_head_class[class_t]->roll_back_map[n-1].is_garbage == 1)
        {
          Roll_Back(log_head_class[class_t]);
        }
      }
    }
    segment = log_head_class[class_t];
  }

  persist_cnt = 0;
  while (segment == nullptr ||
        (ret = segment->AppendBatchFlush(key, value, epoch, &persist_cnt)) ==
            INVALID_VALUE) {
    if (segment) {
      persist_cnt = segment->FlushRemain();
      BatchIndexInsert(persist_cnt, class_t);
      FreezeSegment(segment, class_t);
    }
    segment = db_->log_->NewSegment(class_t);
    if(class_t > 0)
    {
      accumulative_sz_class[class_t] = sz;
      db_->log_->set_class_segment_(class_t, class_seg_working_on[class_t], segment);
    }
    if(class_t == -1) log_head_cold_class0_ = segment;
    else  log_head_class[class_t] = segment;
  }

  queue.push({{i_key, ret}, value.data()});
  if (persist_cnt > 0) {
    BatchIndexInsert(persist_cnt, class_t);
  }

  assert(ret);
  return ret;
}
#else
ValueType DB::Worker::MakeKVItem(const Slice &key, const Slice &value,
                                 int class_t) {
  ValueType ret = INVALID_VALUE;
  uint64_t i_key = *(uint64_t *)key.data();
  uint32_t epoch = db_->GetKeyEpoch(i_key);

  uint32_t sz = sizeof(KVItem) + key.size() + value.size();
  LogSegment *segment = nullptr;

  if(class_t == 0)
  {
    segment = log_head_class[0];
  }
  else if(class_t == -1)
  {
    segment = log_head_cold_class0_;
  }
  else
  {
    accumulative_sz_class[class_t] += sz;
    if(accumulative_sz_class[class_t] > db_->get_threshold(class_t))
    {
      // log_head_class[class_t]->Flush_Header();
      db_->get_class_segment(class_t, worker_id_, 
                             &log_head_class[class_t], &class_seg_working_on[class_t]);
      accumulative_sz_class[class_t] = sz;
      int n = log_head_class[class_t]->num_kvs;
      if(n)
      {
        if(log_head_class[class_t]->roll_back_map[n-1].is_garbage == 1)
        {
          Roll_Back(log_head_class[class_t]);
        }
      }
    }
    segment = log_head_class[class_t];
  }

  while (segment == nullptr
    || (ret = segment->Append(key, value, epoch)) == INVALID_VALUE) {
    FreezeSegment(segment, class_t);
    segment = db_->log_->NewSegment(class_t);
    if(class_t > 0)
    {
      accumulative_sz_class[class_t] = sz;
      db_->log_->set_class_segment_(class_t, class_seg_working_on[class_t], segment);
    }
    if(class_t == -1) log_head_cold_class0_ = segment;
    else  log_head_class[class_t] = segment;
  }

  assert(ret);
  return ret;
}
#endif

void DB::Worker::UpdateIndex(const Slice &key, ValueType tagged_addr, 
                 const Slice &value, int class_t) {
  LogEntryHelper le_helper(tagged_addr);
#ifdef HOT_SC
  if(class_t > 0)
  {
    le_helper.is_hot_sc = true;
    db_->update_hot_sc(key, le_helper, value);
  }
  else 
  {
    db_->index_->Put(key, le_helper);
    if(db_->is_changing())
    {
      db_->hot_set_status_.rcu_progress(worker_id_);
      db_->mark_invalide(key, le_helper);
      db_->hot_set_status_.rcu_exit(worker_id_);
    }
  }
#else
  db_->index_->Put(key, le_helper);
#endif

#ifdef GC_SHORTCUT
#ifdef HOT_COLD_SEPARATE
  if (class_t >= 0) {
    log_head_class[class_t]->AddShortcut(le_helper.shortcut);
  } else {
    log_head_cold_class0_->AddShortcut(le_helper.shortcut);
  }
#else
  log_head_->AddShortcut(le_helper.shortcut);
#endif
#endif

  if (le_helper.old_val != INVALID_VALUE) {
#ifdef HOT_COLD_SEPARATE
    db_->thread_status_.rcu_progress(worker_id_);
    db_->hot_key_set_->Record(key, worker_id_, class_t);
    db_->thread_status_.rcu_exit(worker_id_);
#endif

    MarkGarbage(le_helper.old_val);
  }
}

void DB::Worker::MarkGarbage(ValueType tagged_val) {
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
    assert(segment->roll_back_map[num_].is_garbage == 0);
    segment->roll_back_map[num_].is_garbage = 1;
  }
}

void DB::Worker::Roll_Back(LogSegment *segment)
{
  uint32_t n = segment->num_kvs;
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
  segment->roll_back_tail(roll_back_sz);
  segment->reduce_garbage_bytes(roll_back_sz);
  segment->RB_num_kvs(RB_count);
}

void DB::Worker::FreezeSegment(LogSegment *segment, int class_t) {
  db_->log_->FreezeSegment(segment, class_t);
  if(class_t <= 0) 
    db_->log_->SyncCleanerGarbageBytes(tmp_cleaner_garbage_bytes_);
}
