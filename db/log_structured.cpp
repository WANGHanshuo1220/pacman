#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>

#include "config.h"
#include "log_structured.h"
#include "log_cleaner.h"

// TODO: meta region
LogStructured::LogStructured(std::string db_path, size_t log_size, DB *db,
                             int num_workers, int num_cleaners)
    : num_workers_(num_workers),
      num_cleaners_(4),
      total_log_size_(log_size),
      class0_list_lock_("free_list_class0"),
      class1_list_lock_("free_list_class1"),
      class2_list_lock_("free_list_class2"),
      class3_list_lock_("free_list_class3"),
      num_limit_free_segments_(num_workers * (10 - num_cleaners))
  {
      num_class0_segments_ = (log_size * class0_prop / SEGMENT_SIZE0);
      num_class1_segments_ = (log_size * class1_prop / SEGMENT_SIZE1);
      num_class2_segments_ = (log_size * class2_prop / SEGMENT_SIZE2);
      num_class3_segments_ = (log_size - 
                            num_class0_segments_ * SEGMENT_SIZE0 - 
                            num_class1_segments_ * SEGMENT_SIZE1 - 
                            num_class2_segments_ * SEGMENT_SIZE2 
                            )  / SEGMENT_SIZE3;
      num_segments_ = num_class0_segments_ + 
                      num_class1_segments_ + 
                      num_class2_segments_ +
                      num_class3_segments_;
#ifdef LOG_PERSISTENT
  std::string log_pool_path = db_path + "/log_pool";
  int log_pool_fd = open(log_pool_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (log_pool_fd < 0) {
    ERROR_EXIT("open file failed");
  }
  if (fallocate(log_pool_fd, 0, 0, total_log_size_) != 0) {
    ERROR_EXIT("fallocate file failed");
  }
  pool_start_ = (char *)mmap(NULL, total_log_size_, PROT_READ | PROT_WRITE,
                             MAP_SHARED, log_pool_fd, 0);
  close(log_pool_fd);
#else
  pool_start_ = (char *)mmap(NULL, total_log_size_, PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  madvise(pool_start_, total_log_size_, MADV_HUGEPAGE);
#endif

  printf("num_seg_class0 = %d, num_seg_class1 = %d, " 
         "num_seg_class2 = %d, num_seg_class3 = %d, num_seg = %d\n",
        num_class0_segments_, num_class1_segments_, 
        num_class2_segments_, num_class3_segments_,
        num_segments_);

  if (pool_start_ == nullptr || pool_start_ == MAP_FAILED) {
    ERROR_EXIT("mmap failed");
  }
  LOG("Log: pool_start %p total segments: %d  cleaners: %d\n", pool_start_,
      num_segments_, num_cleaners_);

  // all_segments_.resize(num_segments_ + 2 * num_cleaners_, nullptr);
  all_segments_.resize(num_segments_, nullptr);
  log_cleaners_.resize(num_cleaners_, nullptr);
  // class1_segments_.reserve((int)(num_class1_segments_ * 0.8));
  // class2_segments_.reserve((int)(num_class2_segments_ * 0.8));
  // class3_segments_.reserve((int)(num_class3_segments_ * 0.8));

  int i = 0, j = 0;
  char * pool_start = pool_start_;
  class1_pool_start_ = pool_start_ + num_class0_segments_ * SEGMENT_SIZE0;
  class2_pool_start_ = class1_pool_start_ + num_class1_segments_ * SEGMENT_SIZE1;
  class3_pool_start_ = class2_pool_start_ + num_class2_segments_ * SEGMENT_SIZE2;
  for (i = 0, j = 0; i < num_segments_; i++) {
    // for class0
    if(i < num_class0_segments_ - 1)
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE0, 0);
      free_segments_class0.push(all_segments_[i]);
      pool_start += SEGMENT_SIZE0;
      num_free_list_class0 ++;
    }
    else if(i == num_class0_segments_ - 1)
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE0, 0);
      log_cleaners_[j] = new LogCleaner(db, j, this, all_segments_[i], 0);
      pool_start += SEGMENT_SIZE0;
      all_segments_[i]->set_reserved();
      j++;
    }
    // for class1
    else if(i < (num_class0_segments_ + num_class1_segments_ * 0.8 - 1) )
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE1, 1);
      class1_segments_.push_back(all_segments_[i]);
      pool_start += SEGMENT_SIZE1;
      all_segments_[i]->set_touse();
    }
    else if(i < num_class0_segments_ + num_class1_segments_ - 1)
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE1, 1);
      free_segments_class1.push(all_segments_[i]);
      pool_start += SEGMENT_SIZE1;
      num_free_list_class1 ++;
    }
    else if(i == num_class0_segments_ + num_class1_segments_ - 1)
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE1, 1);
      log_cleaners_[j] = new LogCleaner(db, j, this, all_segments_[i], 1);
      pool_start += SEGMENT_SIZE1;
      all_segments_[i]->set_reserved();
      j++;
    }
    // for class2
    else if(i < (num_class0_segments_ + 
                 num_class1_segments_ + 
                 num_class2_segments_  * 0.8 - 1))
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE2, 2);
      class2_segments_.push_back(all_segments_[i]);
      pool_start += SEGMENT_SIZE2;
      all_segments_[i]->set_touse();
    }
    else if(i < num_class0_segments_ + 
                num_class1_segments_ + 
                num_class2_segments_ - 1)
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE2, 2);
      free_segments_class2.push(all_segments_[i]);
      pool_start += SEGMENT_SIZE2;
      num_free_list_class2 ++;
    }
    else if(i == num_class0_segments_ + 
                 num_class1_segments_ + 
                 num_class2_segments_ - 1)
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE2, 2);
      log_cleaners_[j] = new LogCleaner(db, j, this, all_segments_[i], 2);
      pool_start += SEGMENT_SIZE2;
      all_segments_[i]->set_reserved();
      j++;
    }
    // for class3
    else if(i < (num_class0_segments_ + 
                 num_class1_segments_ + 
                 num_class2_segments_ +
                 num_class3_segments_  * 0.8 - 1))
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE3, 3);
      class3_segments_.push_back(all_segments_[i]);
      pool_start += SEGMENT_SIZE3;
      all_segments_[i]->set_touse();
    }
    else if(i < num_class0_segments_ + 
                num_class1_segments_ + 
                num_class2_segments_ +
                num_class3_segments_ - 1)
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE3, 3);
      free_segments_class3.push(all_segments_[i]);
      pool_start += SEGMENT_SIZE3;
      num_free_list_class3 ++;
    }
    else if(i == num_class0_segments_ + 
                 num_class1_segments_ + 
                 num_class2_segments_ +
                 num_class3_segments_ - 1)
    {
      all_segments_[i] =
          new LogSegment(pool_start, SEGMENT_SIZE3, 3);
      log_cleaners_[j] = new LogCleaner(db, j, this, all_segments_[i], 3);
      all_segments_[i]->set_reserved();
      assert(pool_start - pool_start_ + SEGMENT_SIZE3 == log_size);
    }
  }

  for (int j = 0; j < num_cleaners_; j++) {
    // log_cleaners_[j]->show_closed_list_sz();
    log_cleaners_[j]->StartGCThread();
  }

  if (num_cleaners_ == 0) {
    stop_flag_.store(true, std::memory_order_release);
  }
}

#ifdef INTERLEAVED
  void LogStructured::start_GCThreads()
  {
    for (int j = 0; j < num_cleaners_; j++) {
      log_cleaners_[j]->StartGCThread();
    }
  }
#endif

void LogStructured::get_seg_usage_info()
{
  uint64_t class0 = 0, class1 = 0, class2 = 0, class3 = 0;
  class0 = (num_class0_segments_ - num_free_list_class0) * SEGMENT_SIZE0;
  for(int i = 1; i < class1_segments_.size(); i++)
  {
    class1 += class1_segments_[i]->get_offset();
  }
  for(int i = 1; i < class2_segments_.size(); i++)
  {
    class2 += class2_segments_[i]->get_offset();
  }
  for(int i = 1; i < class3_segments_.size(); i++)
  {
    class3 += class3_segments_[i]->get_offset();
  }

  printf("class0 seg usgae: %ld / %ld = %.3f%%\n",
    class0, num_class0_segments_ * SEGMENT_SIZE0,
    (double)class0/(num_class0_segments_ * SEGMENT_SIZE0));
  printf("class1 seg usage: %ld / %ld = %.3f%%\n",
    class1, num_class1_segments_ * SEGMENT_SIZE1, 
    (double)class1/(num_class1_segments_ * SEGMENT_SIZE1));
  printf("class2 seg usage: %ld / %ld = %.3f%%\n",
    class2, num_class2_segments_ * SEGMENT_SIZE2, 
    (double)class2/(num_class2_segments_ * SEGMENT_SIZE2));
  printf("class3 seg usage: %ld / %ld = %.3f%%\n",
    class3, num_class3_segments_ * SEGMENT_SIZE3, 
    (double)class3/(num_class3_segments_ * SEGMENT_SIZE3));
}

LogStructured::~LogStructured() {
#ifdef GC_EVAL
  get_seg_usage_info();
#endif
  stop_flag_.store(true, std::memory_order_release);
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      log_cleaners_[i]->StopThread();
    }
  }
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      delete log_cleaners_[i];
    }
  }

  for (int i = 0; i < num_segments_; i++) {
    if (all_segments_[i]) {
      delete all_segments_[i];
    }
  }
  all_segments_.clear();

  LOG("num_newSegment %d new_hot %d new_cold %d", num_new_segment_.load(),
      num_new_hot_.load(), num_new_cold_.load());
  munmap(pool_start_, total_log_size_);
  class0_list_lock_.report();
  class1_list_lock_.report();
  class2_list_lock_.report();
  class3_list_lock_.report();
}

#ifdef GC_EVAL
std::pair<int, long> *LogStructured::get_cleaners_info()
{
  int num_cleaners = get_num_cleaners();
  std::pair<int, long> *p = (std::pair<int, long>*)malloc(sizeof(std::pair<int, long>) * num_cleaners);
  for(int i = 0; i < num_cleaners; i ++)
  {
    p[i].first = log_cleaners_[i]->show_GC_times();
    p[i].second = log_cleaners_[i]->show_GC_timecost();
  }
  return p;
}
#endif

LogSegment *LogStructured::NewSegment(int class_) {
  // printf("in NewSegment, %d\n", class_);
  LogSegment *ret = nullptr;
  // uint64_t waiting_time = 0;
  // TIMER_START(waiting_time);
  switch (class_)
  {
    case 0:
      while (true) {
        if (num_free_list_class0 > 0) {
          // printf("  new segment\n");
          std::lock_guard<SpinLock> guard(class0_list_lock_);
          if (!free_segments_class0.empty()) {
            ret = free_segments_class0.front();
            free_segments_class0.pop();
            // printf("  (new seg)%d\n", num_free_segments_.load());
            --num_free_list_class0;
            // printf("  (new seg)%d\n", num_free_segments_.load());
          }
        } else {
          // printf("no new segment\n");
          if (num_cleaners_ == 0) {
            ERROR_EXIT("No free segments and no cleaners");
          }
          usleep(1);
        }
        if (ret) {
          break;
        }
      }
      break;
    case 1:
      while (true) {
        if (num_free_list_class1 > 0) {
          // printf("  new segment\n");
          std::lock_guard<SpinLock> guard(class1_list_lock_);
          if (!free_segments_class1.empty()) {
            ret = free_segments_class1.front();
            free_segments_class1.pop();
            // printf("  (new seg)%d\n", num_free_segments_.load());
            --num_free_list_class1;
            // printf("  (new seg)%d\n", num_free_segments_.load());
          }
        } else {
          // printf("no new segment\n");
          if (num_cleaners_ == 0) {
            ERROR_EXIT("No free segments and no cleaners");
          }
          usleep(1);
        }
        if (ret) {
          break;
        }
      }
      break;
    case 2:
      while (true) {
        if (num_free_list_class2> 0) {
          // printf("  new segment\n");
          std::lock_guard<SpinLock> guard(class2_list_lock_);
          if (!free_segments_class2.empty()) {
            ret = free_segments_class2.front();
            free_segments_class2.pop();
            // printf("  (new seg)%d\n", num_free_segments_.load());
            --num_free_list_class2;
            // printf("  (new seg)%d\n", num_free_segments_.load());
          }
        } else {
          // printf("no new segment\n");
          if (num_cleaners_ == 0) {
            ERROR_EXIT("No free segments and no cleaners");
          }
          usleep(1);
        }
        if (ret) {
          break;
        }
      }
      break;
    case 3:
      while (true) {
        if (num_free_list_class3 > 0) {
          // printf("  new segment\n");
          std::lock_guard<SpinLock> guard(class3_list_lock_);
          if (!free_segments_class3.empty()) {
            ret = free_segments_class3.front();
            free_segments_class3.pop();
            // printf("  (new seg)%d\n", num_free_segments_.load());
            --num_free_list_class3;
            // printf("  (new seg)%d\n", num_free_segments_.load());
          }
        } else {
          // printf("no new segment\n");
          if (num_cleaners_ == 0) {
            ERROR_EXIT("No free segments and no cleaners");
          }
          usleep(1);
        }
        if (ret) {
          break;
        }
      }
      break;
  
    default:
      break;
  }
  // TIMER_STOP(waiting_time);

  COUNTER_ADD_LOGGING(num_new_segment_, 1);
  // not store shortcuts for hot segment
  ret->StartUsing(true);

  // if (hot) {
  //   COUNTER_ADD_LOGGING(num_new_hot_, 1);
  // } else {
  //   COUNTER_ADD_LOGGING(num_new_cold_, 1);
  // }

  // UpdateCleanThreshold();
  return ret;
}

void LogStructured::FreezeSegment(LogSegment *old_segment, int class_) {
  if (old_segment != nullptr && num_cleaners_ > 0) {
    old_segment->Close();
    AddClosedSegment(old_segment, class_);
  }
}

void LogStructured::SyncCleanerGarbageBytes(
    std::vector<size_t> &tmp_garbage_bytes) {
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->cleaner_garbage_bytes_ += tmp_garbage_bytes[i];
    tmp_garbage_bytes[i] = 0;
  }
}

LogSegment *LogStructured::GetSegment(int segment_id) {
  // assert(segment_id < num_segments_ + 2 * num_cleaners_);
  assert(segment_id < num_segments_);
  return all_segments_[segment_id];
}

int LogStructured::GetSegmentID(const char *addr) {
  assert(addr >= pool_start_);
  int seg_id;
  if(addr < class1_pool_start_)
  {
    seg_id = (addr - pool_start_) / SEGMENT_SIZE0;
  }
  else if(addr < class2_pool_start_)
  {
    seg_id = num_class0_segments_ + 
             (addr - class1_pool_start_) / SEGMENT_SIZE1;
  }
  else if(addr < class3_pool_start_)
  {
    seg_id = num_class0_segments_ + num_class1_segments_ +
             (addr - class2_pool_start_) / SEGMENT_SIZE2;
  }
  else
  {
    seg_id = num_class0_segments_ + num_class1_segments_ + num_class2_segments_ +
             (addr - class3_pool_start_) / SEGMENT_SIZE3;
  }
  assert(seg_id < num_segments_);
  return seg_id;
}

int LogStructured::GetSegmentCleanerID(const char *addr) {
  return GetSegmentID(addr) % num_cleaners_;
}

void LogStructured::AddClosedSegment(LogSegment *segment, int class_) {
  // int cleaner_id = GetSegmentCleanerID(segment->get_segment_start());
  assert(class_ >= 0 && class_ <= 3);
  log_cleaners_[class_]->AddClosedSegment(segment);
}

void LogStructured::StartCleanStatistics() {
  start_clean_statistics_time_ = NowMicros();
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->clean_seg_count_before_ =
        log_cleaners_[i]->clean_seg_count_;
    log_cleaners_[i]->clean_time_ns_before_ = log_cleaners_[i]->clean_time_ns_;
  }
}

double LogStructured::GetCompactionCPUUsage() {
  if (num_cleaners_ == 0) {
    return 0.0;
  }

  uint64_t wall_time = NowMicros() - start_clean_statistics_time_;
  uint64_t total_compaction_time = 0;
  for (int i = 0; i < num_cleaners_; i++) {
    total_compaction_time += log_cleaners_[i]->clean_time_ns_ -
                             log_cleaners_[i]->clean_time_ns_before_;
  }
  double usage = (double)total_compaction_time / 1e3 / wall_time;
  return usage;
}

double LogStructured::GetCompactionThroughput() {
  if (num_cleaners_ == 0) {
    return 0.0;
  }

  double total_tp = 0.;
  int SEG_SIZE[4] = {SEGMENT_SIZE0, SEGMENT_SIZE1, SEGMENT_SIZE2, SEGMENT_SIZE3};
  for (int i = 0; i < num_cleaners_; i++) {
    int clean_seg_count = log_cleaners_[i]->clean_seg_count_ -
                          log_cleaners_[i]->clean_seg_count_before_;
    uint64_t clean_time_ns = log_cleaners_[i]->clean_time_ns_ -
                             log_cleaners_[i]->clean_time_ns_before_;
    double tp = 1.0 * clean_seg_count * SEG_SIZE[i] * 1e9 /
                clean_time_ns;
    total_tp += tp;
  }
  return total_tp;
}

void LogStructured::UpdateCleanThreshold() {
  int cnt = alloc_counter_.fetch_add(1);

  if (!FS_flag_.test_and_set()) {
    int num_free = num_free_segments_.load(std::memory_order_relaxed);
    double step = num_segments_ / 200.;  // 0.5%
    double thresh_free = num_segments_ * clean_threshold_ / 100.;

    switch (free_status_) {
      case FS_Sufficient:
        if (num_free < thresh_free) {
          free_status_ = FS_Trigger;
          alloc_counter_ = 0;
        }
        break;
      case FS_Trigger:
        if (num_free < thresh_free - num_workers_ ||
            num_free < num_limit_free_segments_) {
          alloc_counter_ = 0;
          if (num_free > thresh_free - step && clean_threshold_ < 10) {
            // if num_free is much less than thresh, doesn't need to change
            // thresh
            ++clean_threshold_;
          }
          free_status_ = FS_Insufficient;
        } else if (num_free > thresh_free &&
                   thresh_free - step > num_limit_free_segments_ &&
                   clean_threshold_ > 1 && cnt > num_workers_ * 4) {
          alloc_counter_ = 0;
          --clean_threshold_;
          free_status_ = FS_Sufficient;
        }
        break;
      case FS_Insufficient:
        if (num_free >= thresh_free) {
          free_status_ = FS_Sufficient;
        }
        break;
      default:
        break;
    }
    FS_flag_.clear();
  }
}


void LogStructured::RecoverySegments(DB *db) {
  // // stop compaction first.
  // stop_flag_.store(true, std::memory_order_release);
  // for (int i = 0; i < num_cleaners_; i++) {
  //   if (log_cleaners_[i]) {
  //     log_cleaners_[i]->StopThread();
  //   }
  // }
  // for (int i = 0; i < num_cleaners_; i++) {
  //   if (log_cleaners_[i]) {
  //     delete log_cleaners_[i];
  //     log_cleaners_[i] = nullptr;
  //   }
  // }

  // assert(free_segments_.size() == num_free_segments_.load());
  // printf("before recovery: %ld free segments\n", free_segments_.size());
  
  // // clean segments, simulate a clean restart.
  // while (!free_segments_.empty()) {
  //   free_segments_.pop();
  // }
  // num_free_segments_ = 0;

  // for (int i = 0; i < num_segments_; i++) {
  //   if (all_segments_[i]) {
  //     delete all_segments_[i];
  //   }
  // }
  // all_segments_.clear();
  // all_segments_.resize(num_segments_, nullptr);

  // recovery_counter_ = 0;
  
  // // start recovery
  // for (int i = 0; i < num_cleaners_; i++) {
  //   log_cleaners_[i] = new LogCleaner(db, i, this, nullptr);
  // }

  // uint64_t rec_time = 0;
  // TIMER_START(rec_time);
  // for (int i = 0; i < num_cleaners_; i++) {
  //   log_cleaners_[i]->StartRecoverySegments();
  // }

  // // wait for recovery over
  // {
  //   std::unique_lock<std::mutex> lk(rec_mu_);
  //   while (recovery_counter_ < num_cleaners_) {
  //     rec_cv_.wait(lk);
  //   }
  // }
  // TIMER_STOP(rec_time);
  // printf("after recovery: %ld free segments\n", free_segments_.size());
  // assert(free_segments_.size() == num_free_segments_.load());
  // printf("recovery segments time %lu us\n", rec_time / 1000);
}

void LogStructured::RecoveryInfo(DB *db) {
  // if (recovery_counter_ != num_cleaners_) {
  //   ERROR_EXIT("Should recovery segments first");
  // }
  // recovery_counter_ = 0;
  // uint64_t rec_time = 0;
  // TIMER_START(rec_time);
  // for (int i = 0; i < num_cleaners_; i++) {
  //   log_cleaners_[i]->StartRecoveryInfo();
  // }

  // // wait for recovery over
  // {
  //   std::unique_lock<std::mutex> lk(rec_mu_);
  //   while (recovery_counter_ < num_cleaners_) {
  //     rec_cv_.wait(lk);
  //   }
  // }
  // TIMER_STOP(rec_time);
  // printf("recovery info time %lu us\n", rec_time / 1000);
}


void LogStructured::RecoveryAll(DB *db) {
//   // stop compaction first.
//   stop_flag_.store(true, std::memory_order_release);
//   for (int i = 0; i < num_cleaners_; i++) {
//     if (log_cleaners_[i]) {
//       log_cleaners_[i]->StopThread();
//     }
//   }
//   for (int i = 0; i < num_cleaners_; i++) {
//     if (log_cleaners_[i]) {
//       delete log_cleaners_[i];
//       log_cleaners_[i] = nullptr;
//     }
//   }

//   assert(free_segments_.size() == num_free_segments_.load());
//   printf("before recovery: %ld free segments\n", free_segments_.size());
//   // clear old segments
//   while (!free_segments_.empty()) {
//     free_segments_.pop();
//   }
//   num_free_segments_ = 0;

//   for (int i = 0; i < num_segments_; i++) {
//     if (all_segments_[i]) {
//       delete all_segments_[i];
//     }
//   }
//   all_segments_.clear();
//   all_segments_.resize(num_segments_, nullptr);

//   // delete old index
// #ifndef IDX_PERSISTENT
//   db->NewIndexForRecoveryTest();
// #endif

//   recovery_counter_ = 0;
  
//   // start recovery
//   for (int i = 0; i < num_cleaners_; i++) {
//     log_cleaners_[i] = new LogCleaner(db, i, this, nullptr);
//   }

//   uint64_t rec_time = 0;
//   TIMER_START(rec_time);
//   for (int i = 0; i < num_cleaners_; i++) {
//     log_cleaners_[i]->StartRecoveryAll();
//   }

//   // wait for recovery over
//   {
//     std::unique_lock<std::mutex> lk(rec_mu_);
//     while (recovery_counter_ < num_cleaners_) {
//       rec_cv_.wait(lk);
//     }
//   }
//   TIMER_STOP(rec_time);
//   printf("after recovery: %ld free segments\n", free_segments_.size());
//   assert(free_segments_.size() == num_free_segments_.load());
//   printf("recovery all time %lu us\n", rec_time / 1000);
}
