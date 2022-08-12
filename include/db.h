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
    long update_index_time = 0;
    long   update_idx_p1 = 0;
    long   MarkGarbage_time = 0;
    long     markgarbage_p1 = 0;
    long     markgarbage_p2 = 0;
    int max_kv_sz = 0;
#endif
#ifdef INTERLEAVED
    int num_workers = 1;
    int cur_hot_segment_ = 0;
    int cur_cold_segment_ = 0;
    int num_hot_segments_;
    int num_cold_segments_;
    int change_seg_threshold = (SEGMENT_SIZE / 2) / num_workers;
    int accumulative_sz_hot = 0;
    int accumulative_sz_cold = 0;
    int roll_back_count = 0;
#endif

  // only for test
  // int test_count = 0;
  
   private:
    int worker_id_;
    DB *db_;
    LogSegment *log_head_ = nullptr;
#ifdef HOT_COLD_SEPARATE
    LogSegment *cold_log_head_ = nullptr;
#endif

    // lazily update garbage bytes for cleaner, avoid too many FAAs
    std::vector<size_t> tmp_cleaner_garbage_bytes_;

    ValueType MakeKVItem(const Slice &key, const Slice &value, bool hot);
    void UpdateIndex(const Slice &key, ValueType val, bool hot);
    void MarkGarbage(ValueType tagged_val);
    void FreezeSegment(LogSegment *segment);

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

#ifdef INTERLEAVED
  void StopRBThread() {
    if (roll_back_thread_.joinable()) {
      roll_back_thread_.join();
    }
  }

  void StartRBThread() {
    // StopRBThread();
    roll_back_thread_ = std::thread(&DB::roll_back_, this);
  }

  bool is_roll_back_list_empty() { return roll_back_list.empty(); }
  LogSegment *get_front_roll_back_list() { return roll_back_list.front(); }
  void pop_front_roll_back_list() { roll_back_list.pop(); }
  void push_back_roll_back_list(LogSegment *seg) { roll_back_list.push(seg); }
#endif

  // GC_EVAL
#ifdef GC_EVAL
  LogStructured *get_log_() { return log_; }
#endif

 private:
#ifdef INTERLEAVED
  std::queue<LogSegment *> roll_back_list;
  std::thread roll_back_thread_;
  std::atomic<bool> stop_flag_RB{false};
#endif
  Index *index_;
  LogStructured *log_;
  const int num_workers_;
  const int num_cleaners_;
  std::atomic<int> cur_num_workers_{0};
#ifdef HOT_COLD_SEPARATE
  HotKeySet *hot_key_set_ = nullptr;
#endif
  ThreadStatus thread_status_;

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
