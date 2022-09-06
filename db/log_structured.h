#pragma once

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <tuple>
#include <vector>
#include <memory>

#include "segment.h"
#include "util/lock.h"
#include "util/util.h"

class DB;
class LogCleaner;

enum FreeStatus { FS_Sufficient, FS_Trigger, FS_Insufficient };

class LogStructured {
 public:
  explicit LogStructured(std::string db_path, size_t log_size, DB *db,
                         int num_workers, int num_cleaners);
  ~LogStructured();

  LogSegment *NewSegment(int class_);
  void FreezeSegment(LogSegment *old_segment, int class_);
  void SyncCleanerGarbageBytes(std::vector<size_t> &tmp_garbage_bytes);
  LogSegment *GetSegment(int segment_id);
  int GetSegmentID(const char *addr);
  int GetSegmentCleanerID(const char *addr);

  void StartCleanStatistics();
  double GetCompactionCPUUsage();
  double GetCompactionThroughput();
  void RecoverySegments(DB *db);
  void RecoveryInfo(DB *db);
  void RecoveryAll(DB *db);

  uint64_t get_num_class_segments_(int class_) { return class_segments_[class_].size(); }
  LogSegment **get_class_segment_(int class_, int i) {return &class_segments_[class_][i]; } 
  void set_class_segment_(int class_, uint32_t i, LogSegment *s) 
  { 
    printf("%d, %d, %p\n", class_, i, s);
    class_segments_[class_][i] = s; 
  } 
  void start_GCThreads();

  // GC_EVAL
#ifdef GC_EVAL
  std::pair<int, long> *get_cleaners_info();
  int get_num_cleaners() { return num_cleaners_; }
  int get_num_segments_() { return num_segments_; }
  LogSegment *get_segments_(int i) { return all_segments_[i]; }
#endif
  void get_seg_usage_info();

  // char *get_pool_start() { return pool_start_; }
 private:
  const int num_workers_;
  const int num_cleaners_;
  char *class_pool_start_[num_class];
  const size_t total_log_size_;
  int num_segments_ = 0;
  // SpinLock reserved_list_lock_;
  std::atomic<bool> stop_flag_{false};
  // const int max_reserved_segments_;
  SpinLock class_list_lock_[num_class];

  int num_class_segments_[num_class];

  std::atomic<int> num_free_list_class[num_class] = {0, 0, 0, 0};

  std::vector<LogSegment *> all_segments_;
  std::vector<LogCleaner *> log_cleaners_;

  std::vector<std::queue<LogSegment *>> free_segments_class{num_class};

  std::vector<std::vector<LogSegment *>> class_segments_{num_class};

  const float class0_prop = 0.4;
  const float class1_prop = 0.2;
  const float class2_prop = 0.2;
  const float class3_prop = 1. - class0_prop - class1_prop - class2_prop;
  const float class_prop[num_class] = {class0_prop, class1_prop, class2_prop, class3_prop};
  uint64_t new_count = 0;
  // std::queue<LogSegment *> reserved_segments_;

  std::atomic<int> num_free_segments_{0};
  std::atomic<int> alloc_counter_{0};
  const int num_limit_free_segments_;
  volatile int clean_threshold_ = 20;

  volatile FreeStatus free_status_ = FS_Sufficient;
  std::atomic_flag FS_flag_{ATOMIC_FLAG_INIT};

  std::atomic<int> recovery_counter_{0};
  std::mutex rec_mu_;
  std::condition_variable rec_cv_;

  // interleaved_GC
  int cur_segment;
  int get_cur_segment() { return cur_segment; }

  // statistics
#ifdef LOGGING
  std::atomic<int> num_new_segment_{0};
  std::atomic<int> num_new_hot_{0};
  std::atomic<int> num_new_cold_{0};
#endif
  uint64_t start_clean_statistics_time_ = 0;

  void AddClosedSegment(LogSegment *segment, int class_);
  // void LockFreeList() { free_list_lock_.lock(); }
  // void UnlockFreeList() { free_list_lock_.unlock(); }
  void UpdateCleanThreshold();


  // LogSegment *NewReservedSegment() {
  //   std::lock_guard<SpinLock> guard(reserved_list_lock_);
  //   LogSegment *segment = nullptr;
  //   if (unlikely(reserved_segments_.empty())) {
  //     ERROR_EXIT("no reserved segment left");
  //   } else {
  //     segment = reserved_segments_.front();
  //     reserved_segments_.pop();
  //   }
  //   assert(segment);
  //   return segment;
  // }

  // bool TryAddReservedSegment(LogSegment *segment) {
  //   std::lock_guard<SpinLock> guard(reserved_list_lock_);
  //   bool ret = reserved_segments_.size() < max_reserved_segments_;
  //   if (ret) {
  //     reserved_segments_.push(segment);
  //   }
  //   return ret;
  // }

  friend class LogCleaner;
  friend class DB;
  


  DISALLOW_COPY_AND_ASSIGN(LogStructured);
};
