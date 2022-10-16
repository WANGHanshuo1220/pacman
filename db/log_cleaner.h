#pragma once

#include <vector>
#include <queue>
#include <unordered_set>

#include "config.h"
#include "db.h"
#include "log_structured.h"
#include "util/util.h"


class LogCleaner {
 public:
  std::atomic<size_t> cleaner_garbage_bytes_{0};
// #ifdef GC_EVAL
  int GC_times = 0;
  std::atomic<long> GC_timecost = 0; // us
  std::atomic<int> help = 0;
  int get_cleaner_id() { return cleaner_id_; }
  int show_GC_times() 
  { 
    return GC_times;
  }
  long show_GC_timecost()
  { 
    return GC_timecost.load(std::memory_order_relaxed);
  }
// #endif

  // clean statistics
#ifdef LOGGING
  uint64_t clean_garbage_bytes_ = 0;
  uint64_t clean_total_bytes_ = 0;
  uint64_t hot_clean_garbage_bytes_ = 0;
  uint64_t hot_clean_total_bytes_ = 0;
  uint64_t cold_clean_garbage_bytes_ = 0;
  uint64_t cold_clean_total_bytes_ = 0;
  uint64_t copy_time_ = 0;
  uint64_t update_index_time_ = 0;
  uint64_t check_liveness_time_ = 0;
  uint64_t pick_time_ = 0;
  int clean_hot_count_ = 0;
  int clean_cold_count_ = 0;
  int flush_pass_ = 0;
  int move_count_ = 0;
  int garbage_move_count_ = 0;
  int fast_path_ = 0;
  int shortcut_cnt_ = 0;
#endif

  int clean_seg_count_ = 0;
  uint64_t clean_time_ns_ = 0;
  int clean_seg_count_before_ = 0;
  uint64_t clean_time_ns_before_ = 0;
  uint64_t clean_sort_ns_time_ = 0;
  uint64_t clean_sort_us_before_ = 1000;
#ifdef GC_EVAL
  uint64_t DoMemoryClean_time_ns_ = 0;
  uint64_t DoMemoryClean_time1_ns_ = 0;
  uint64_t DoMemoryClean_time2_ns_ = 0;
  uint64_t DoMemoryClean_time3_ns_ = 0;
  uint64_t CompactionSeg_time_ns_ = 0;
  uint64_t CompactionSeg_time1_ns_ = 0;
  uint64_t CompactionSeg_time1_1_ns_ = 0;
  uint64_t CompactionSeg_time1_2_ns_ = 0;
  uint64_t CompactionSeg_time2_ns_ = 0;
#endif

  LogCleaner(DB *db, int cleaner_id, LogStructured *log,
             LogSegment *reserved_segment, int class__)
      : db_(db),
        cleaner_id_(cleaner_id),
        log_(log),
        reserved_segment_(reserved_segment),
        class_(class__),
        list_lock_(std::string("gc_list_lock_") + std::to_string(cleaner_id)) 
  {
    tmp_cleaner_garbage_bytes_.resize(db->num_cleaners_, 0);
    if (reserved_segment_) {
      reserved_segment_->StartUsing(false);
      reserved_segment_->set_reserved();
      assert(reserved_segment_->is_segment_reserved());
    }
#ifdef BATCH_COMPACTION
    volatile_segment_ = new VirtualSegment(SEGMENT_SIZE[class_]);
    volatile_segment_->set_has_shortcut(false);
#endif
  }

  ~LogCleaner() {
// #ifndef GC_EVAL
    printf("%dth cleaner(%d): GC_times = %d, "
           "clean_time_ns_ = %ldns (%.3f s), "
           "clean_sort_ns_ = %ldns (%.3f s)\n", 
      get_cleaner_id(), help.load(), show_GC_times(), 
      clean_time_ns_, (float)clean_time_ns_/1000000000,
      clean_sort_ns_time_, (float)clean_sort_ns_time_/1000000000);
// #else
    // printf("%dth cleaner: GC_times = %d\n", get_cleaner_id(), show_GC_times());
    // printf("  clean_time_ns_               = %ldns (%.3f s)\n", 
    //   clean_time_ns_, (float)clean_time_ns_/1000000000);
    // printf("    DomemoryClean_times_ns_    = %ldns (%.3f s)\n", 
    //   DoMemoryClean_time_ns_, (float)DoMemoryClean_time_ns_/1000000000);
    // printf("      DomemoryClean_times1_ns_ = %ldns (%.3f s)\n", 
    //   DoMemoryClean_time1_ns_, (float)DoMemoryClean_time1_ns_/1000000000);
    // printf("      DomemoryClean_times2_ns_ = %ldns (%.3f s)\n", 
    //   DoMemoryClean_time2_ns_, (float)DoMemoryClean_time2_ns_/1000000000);
    // printf("      DomemoryClean_times3_ns_ = %ldns (%.3f s)\n", 
    //   DoMemoryClean_time3_ns_, (float)DoMemoryClean_time3_ns_/1000000000);
    // printf("      Compaction_times_ns_     = %ldns (%.3f s)\n", 
    //   CompactionSeg_time_ns_, (float)CompactionSeg_time_ns_/1000000000);
    // printf("        Compaction_times1_ns_  = %ldns (%.3f s)\n", 
    //   CompactionSeg_time1_ns_, (float)CompactionSeg_time1_ns_/1000000000);
    // printf("          Compaction_times1_1_ns_  = %ldns (%.3f s)\n", 
    //   CompactionSeg_time1_1_ns_, (float)CompactionSeg_time1_1_ns_/1000000000);
    // printf("          Compaction_times1_2_ns_  = %ldns (%.3f s)\n", 
    //   CompactionSeg_time1_2_ns_, (float)CompactionSeg_time1_2_ns_/1000000000);
    // printf("        Compaction_times2_ns_  = %ldns (%.3f s)\n", 
    //   CompactionSeg_time2_ns_, (float)CompactionSeg_time2_ns_/1000000000);
// #endif
#ifdef BATCH_COMPACTION
    delete volatile_segment_;
#endif
    LOG("cleaner %d: clean %d (hot %d cold %d) flush_pass %d move %d "
        "move_garbage %d shortcut_cnt %d fast_path %d (%.1lf%%) pick_time %lu "
        "copy_time %lu "
        "check_liveness_time_ %lu update_index_time %lu clean_time_ns %lu "
        "avg_garbage_proportion %.1f%% "
        "hot %.1f%% cold %.1f%%",
        cleaner_id_, clean_seg_count_, clean_hot_count_, clean_cold_count_,
        flush_pass_, move_count_, garbage_move_count_, shortcut_cnt_,
        fast_path_, 100. * fast_path_ / move_count_, pick_time_ / 1000,
        copy_time_ / 1000, check_liveness_time_ / 1000,
        update_index_time_ / 1000, clean_time_ns_ / 1000,
        clean_garbage_bytes_ * 100.f / clean_total_bytes_,
        hot_clean_garbage_bytes_ * 100.f / hot_clean_total_bytes_,
        cold_clean_garbage_bytes_ * 100.f / cold_clean_total_bytes_);

    list_lock_.report();
    if (reserved_segment_) {
#if defined(LOG_BATCHING) && !defined(BATCH_COMPACTION)
      reserved_segment_->FlushRemain();
      BatchIndexUpdate();
#endif
      log_->FreezeSegment(reserved_segment_, class_);
    }
    if (backup_segment_) {
      std::lock_guard<SpinLock> guard(log_->class_list_lock_[class_]);
      log_->free_segments_class[class_].push(backup_segment_);
      ++log_->num_free_list_class[class_];
    }

    closed_segments_.clear();
    to_compact_segments_.clear();
  }

  uint64_t get_closed_list_sz() 
  { 
    return closed_segments_.size();
  }

  void StopThread() {
    if (gc_thread_.joinable()) {
      gc_thread_.join();
    }
  }

  void StartGCThread() {
    StopThread();
    gc_thread_ = std::thread(&LogCleaner::CleanerEntry, this);
  }

  void StartRecoverySegments() {
    StopThread();
    gc_thread_ = std::thread(&LogCleaner::RecoverySegments, this);
  }

  void StartRecoveryInfo() {
    StopThread();
    gc_thread_ = std::thread(&LogCleaner::RecoveryInfo, this);
  }

  void StartRecoveryAll() {
    StopThread();
    gc_thread_ = std::thread(&LogCleaner::RecoveryAll, this);
  }

  void AddClosedSegment(LogSegment *segment, int class_t) {
    if(class_t > 0)
    {
      LockUsedList();
      closed_segments_.push_back(segment);
      UnlockUsedList();
    }
    else
    {
      LockUsedList();
      if(class_t == 0) closed_segments_.push_back(segment);
      else closed_cold_segments_.push_back({segment, 0});
      UnlockUsedList();
    }
  }

  std::vector<int> range = {100, 75, 50, 25, 0};
  int gap[2] = {10, 5};
  int sort_range[2];
  int num_worker;
  int worker_range;
  std::vector<uint32_t> num_class_segs;

 private:
  DB *db_;
  int cleaner_id_;
  LogStructured *log_;
  std::thread gc_thread_;
  VirtualSegment *volatile_segment_ = nullptr;
  LogSegment *reserved_segment_ = nullptr;
  LogSegment *backup_segment_ = nullptr;  // prevent gc dead lock
  std::vector<ValidItem> valid_items_;
  std::vector<size_t> tmp_cleaner_garbage_bytes_;
  double last_update_time_ = 0.;
  std::list<LogSegment *> closed_segments_;
  std::list<SegmentInfo> closed_cold_segments_;
  std::list<LogSegment *> to_compact_segments_;
  std::list<SegmentInfo> to_compact_cold_segments_;
  SpinLock list_lock_;
  const uint32_t class_;

  uint32_t free_count = 0;
  uint32_t num_new = 0;

  bool IsGarbage(KVItem *kv) {
    int log_id = log_->GetSegmentID(reinterpret_cast<char *>(kv));
    return log_->all_segments_[log_id]->IsGarbage(reinterpret_cast<char *>(kv));
  }
  void LockUsedList() { list_lock_.lock(); }
  void UnlockUsedList() { list_lock_.unlock(); }

  void CleanerEntry();
  bool NeedCleaning();
  void BatchFlush();
  void BatchIndexUpdate();
  void CopyValidItemToBuffer0(LogSegment *segment);
  void CopyValidItemToBuffer123(LogSegment *segment);
  void BatchCompactSegment(LogSegment *segment);
  void CompactSegment0(LogSegment *segment);
  void CompactSegment123(LogSegment *segment);
  void FreezeReservedAndGetNew();
  void MarkGarbage0(ValueType tagged_val);
  void MarkGarbage123(ValueType tagged_val);
  void DoMemoryClean();
  void Sort_for_worker(int worker_i,
                       int sort_begin, int i);
  void Help_sort(int i);

  void RecoverySegments();
  void RecoveryInfo();
  void RecoveryAll();

  DISALLOW_COPY_AND_ASSIGN(LogCleaner);
};
