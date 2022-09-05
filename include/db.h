#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <queue>
#include <utility>
#include <thread>

#include "config.h"
#include "slice.h"
#include "db_common.h"
#include "util/util.h"
#include "util/thread_status.h"
#include "util/index_arena.h"
#include "../db/log_structured.h"
#include "../db/CircleQueue.h"

// index operations
class Index {
 public:
  virtual ~Index(){};
  virtual ValueType Get(const Slice &key) = 0;
  virtual void Put(const Slice &key, LogEntryHelper &le_helper) = 0;
  virtual void Delete(const Slice &key) = 0;
  virtual void Scan(const Slice &key, int cnt, std::vector<ValueType> &vec) {
    ERROR_EXIT("not supported in this class");
  }
  virtual void GCMove(const Slice &key, LogEntryHelper &le_helper) = 0;
  virtual void PrefetchEntry(const Shortcut &sc) {}
};

class LogSegment;
class LogStructured;
class HotKeySet;
class DB {
 public:
  class Worker {
   public:
    explicit Worker(DB *db);
    ~Worker();

    bool Get(const Slice &key, std::string *value);
    void Put(const Slice &key, const Slice &value);
    size_t Scan(const Slice &key, int cnt);
    bool Delete(const Slice &key);
    // int show_ID() {return worker_id_; };

#ifdef GC_EVAL
    long check_hotcold_time = 0;
    long insert_time = 0;
    long   change_seg_time= 0;
    long   append_time = 0;
    long     set_seg_time = 0;
    long update_index_time = 0;
    long   update_idx_p1 = 0;
    long   MarkGarbage_time = 0;
    long     markgarbage_p1 = 0;
    long     markgarbage_p2 = 0;
    int max_kv_sz = 0;
    long get_kv_num = 0;
    long get_kv_sz = 0;
#endif
    // int num_hot_segments_;
    // int num_cold_segments_;
#ifdef LOG_BATCHING
    uint32_t change_seg_threshold = LOG_BATCHING_SIZE;
    bool hot_batch_persistent = false;
    bool cold_batch_persistent = false;
#endif
    uint32_t class_seg_working_on[num_class];
    uint32_t accumulative_sz_class[num_class] = {0};
    // int roll_back_count = 0;

  // only for test
  // int test_count = 0;
  
   private:
    int worker_id_;
    DB *db_;
    LogSegment *log_head_class[num_class] = {nullptr};

    // lazily update garbage bytes for cleaner, avoid too many FAAs
    std::vector<size_t> tmp_cleaner_garbage_bytes_;

    ValueType MakeKVItem(const Slice &key, const Slice &value, int class_);
    void UpdateIndex(const Slice &key, ValueType val, int class_);
    void MarkGarbage(ValueType tagged_val);
    void FreezeSegment(LogSegment *segment, int class_);

#ifdef LOG_BATCHING
    void BatchIndexInsert(int cnt, bool hot);

    std::queue<std::pair<KeyType, ValueType>> buffer_queue_;
#ifdef HOT_COLD_SEPARATE
    std::queue<std::pair<KeyType, ValueType>> cold_buffer_queue_;
#endif
#endif

    DISALLOW_COPY_AND_ASSIGN(Worker);
  };

  DB(std::string pool_path, size_t log_size, int num_workers, int num_cleaners);
  virtual ~DB();

  std::unique_ptr<Worker> GetWorker() {
    return std::make_unique<Worker>(this);
  }

  // statistics
  void StartCleanStatistics();
  double GetCompactionCPUUsage();
  double GetCompactionThroughput();

  // recovery
  void RecoverySegments();
  void RecoveryInfo();
  void RecoveryAll();
  void NewIndexForRecoveryTest();

  void start_GCThreads();

  void StopRBThread() {
    for(int i = 0; i < 2; i++)
    {
      if (roll_back_thread_[i].joinable()) {
        roll_back_thread_[i].join();
      }
    }
  }

  // void StartRBThread() {
  //   StopRBThread();
  //   printf("start RB Thread1\n");
  //   roll_back_thread_[0] = std::thread(&DB::roll_back_, this);
  //   printf("start RB Thread2\n");
  //   roll_back_thread_[1] = std::thread(&DB::roll_back_, this);
  // }

  bool is_roll_back_list_empty() { return roll_back_list.empty(); }
  void enque_roll_back_queue(LogSegment * s) { roll_back_queue.CQ_enque(s); }
  LogSegment *deque_roll_back_queue() { return roll_back_queue.CQ_deque(); }

  std::pair<uint32_t, LogSegment **> get_class_segment(int class_)
  {
    std::lock_guard<SpinLock> guard(class_segment_list_lock[class_]);
    std::pair<uint32_t, LogSegment **> ret;
    ret.second = log_->get_class_segment_(class_, next_class_segment_[class_]);
    ret.first = next_class_segment_[class_];
    (*ret.second)->set_using();
    next_class_segment_[class_] ++;
    assert(next_class_segment_[class_] <= db_num_class_segs[class_]);
    if(next_class_segment_[class_] == db_num_class_segs[class_])
    {
      next_class_segment_[class_] = 0;
    }
    return ret;
  }

  // GC_EVAL
#ifdef GC_EVAL
  LogStructured *get_log_() { return log_; }
#endif
  uint32_t get_threshold(int class_) { return change_seg_threshold_class[class_]; }

 private:
  std::queue<LogSegment *> roll_back_list;
  std::thread roll_back_thread_[2];
  std::atomic<bool> stop_flag_RB{false};
  CircleQueue roll_back_queue; 
  Index *index_;
  LogStructured *log_;
  const int num_workers_;
  const int num_cleaners_;
  std::atomic<int> cur_num_workers_{0};
  HotKeySet *hot_key_set_ = nullptr;
  ThreadStatus thread_status_;
  std::atomic<int> next_class_segment_[num_class] = {0, 0, 0, 0};
  uint64_t change_seg_threshold_class[num_class];
  SpinLock class_segment_list_lock[num_class];
  uint64_t db_num_class_segs[num_class] = {0};
  long roll_back_count = 0;
  uint64_t roll_back_bytes = 0;

  static constexpr int EPOCH_MAP_SIZE = 1024;
  std::array<std::atomic_uint_fast32_t, EPOCH_MAP_SIZE> epoch_map_{};

  // // index operations
  // virtual ValueType IndexGet(const Slice &key) = 0;
  // virtual void IndexPut(const Slice &key, LogEntryHelper &le_helper) = 0;
  // virtual void IndexDelete(const Slice &key) = 0;
  // virtual void IndexScan(const Slice &key, int cnt,
  //                        std::vector<ValueType> &vec) {
  //   ERROR_EXIT("not supported");
  // }
  // virtual void GCMove(const Slice &key, LogEntryHelper &le_helper) = 0;
  // virtual void PrefetchEntry(const Shortcut &sc) {}

  uint32_t GetKeyEpoch(uint64_t i_key) {
    size_t idx = i_key % EPOCH_MAP_SIZE;
    return epoch_map_[idx].fetch_add(1, std::memory_order_relaxed);
  }

#ifdef INTERLEAVED
  void roll_back_();
#endif

  friend class LogCleaner;
  friend class HotKeySet;

  DISALLOW_COPY_AND_ASSIGN(DB);
};
