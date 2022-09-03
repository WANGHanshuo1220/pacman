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
    uint32_t class1_seg_working_on;
    uint32_t class2_seg_working_on;
    uint32_t class3_seg_working_on;
#ifdef LOG_BATCHING
    uint32_t change_seg_threshold = LOG_BATCHING_SIZE;
    bool hot_batch_persistent = false;
    bool cold_batch_persistent = false;
#else
    uint64_t change_seg_threshold_class1 = SEGMENT_SIZE1 / 2;
    uint64_t change_seg_threshold_class2 = SEGMENT_SIZE2 / 2;
    uint64_t change_seg_threshold_class3 = SEGMENT_SIZE3 / 2;
#endif
    uint32_t accumulative_sz_class1 = 0;
    uint32_t accumulative_sz_class2 = 0;
    uint32_t accumulative_sz_class3 = 0;
    // int roll_back_count = 0;

  // only for test
  // int test_count = 0;
  
   private:
    int worker_id_;
    DB *db_;
    LogSegment *log_head_class0 = nullptr;
    LogSegment *log_head_class1 = nullptr;
    LogSegment *log_head_class2 = nullptr;
    LogSegment *log_head_class3 = nullptr;

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

  std::pair<int, LogSegment **> get_class1_segment()
  {
    std::lock_guard<SpinLock> guard(class1_segment_list);
    std::pair<int, LogSegment **> ret;
    ret.second = log_->get_class1_segment_(next_class1_segment_);
    ret.first = next_class1_segment_;
    (*ret.second)->set_using();
    next_class1_segment_ ++;
    if(next_class1_segment_ == db_num_class1_segs)
    {
      next_class1_segment_ = 0;
    }
    return ret;
  }

  std::pair<int, LogSegment **> get_class2_segment()
  {
    std::lock_guard<SpinLock> guard(class2_segment_list);
    std::pair<int, LogSegment **> ret;
    ret.second = log_->get_class2_segment_(next_class2_segment_);
    ret.first = next_class2_segment_;
    (*ret.second)->set_using();
    next_class2_segment_ ++;
    if(next_class2_segment_ == db_num_class2_segs)
    {
      next_class2_segment_ = 0;
    }
    return ret;
  }

  std::pair<int, LogSegment **> get_class3_segment()
  {
    std::lock_guard<SpinLock> guard(class3_segment_list);
    std::pair<int, LogSegment **> ret;
    ret.second = log_->get_class3_segment_(next_class3_segment_);
    ret.first = next_class3_segment_;
    (*ret.second)->set_using();
    next_class3_segment_ ++;
    if(next_class3_segment_ == db_num_class3_segs)
    {
      next_class3_segment_ = 0;
    }
    return ret;
  }

  // GC_EVAL
#ifdef GC_EVAL
  LogStructured *get_log_() { return log_; }
#endif

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
  std::atomic<int> next_class1_segment_;
  std::atomic<int> next_class2_segment_;
  std::atomic<int> next_class3_segment_;
  SpinLock class1_segment_list;
  SpinLock class2_segment_list;
  SpinLock class3_segment_list;
  int db_num_class1_segs = 0;
  int db_num_class2_segs = 0;
  int db_num_class3_segs = 0;
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
