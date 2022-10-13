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
      num_cleaners_(num_cleaners),
      total_log_size_(log_size),
      num_limit_free_segments_(num_workers * (10 - num_cleaners))
  {
    for(int i = 0; i < num_class; i++)
    {
      std::string name = "free_list_class" + std::to_string(i);
      class_list_lock_[i].set_name(name);
    }
    uint64_t sum = 0;
    for(int i = 0; i < num_class - 1; i ++)
    {
      num_class_segments_[i] = (log_size * class_prop[i] / SEGMENT_SIZE[i]);
      sum += num_class_segments_[i] * SEGMENT_SIZE[i];
    }
    num_class_segments_[num_class - 1] = (log_size - sum)
                                          / SEGMENT_SIZE[num_class - 1];
    for(int i = 0; i < num_class; i++)
    {
      num_segments_ += num_class_segments_[i];
    }
#ifdef LOG_PERSISTENT
  std::string log_pool_path = db_path + "/log_pool";
  int log_pool_fd = open(log_pool_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (log_pool_fd < 0) {
    ERROR_EXIT("open file failed");
  }
  if (fallocate(log_pool_fd, 0, 0, total_log_size_) != 0) {
    ERROR_EXIT("fallocate file failed");
  }
  class_pool_start_[0] = (char *)mmap(NULL, total_log_size_, PROT_READ | PROT_WRITE,
                             MAP_SHARED, log_pool_fd, 0);
  close(log_pool_fd);
#else
  class_pool_start_[0] = (char *)mmap(NULL, total_log_size_, PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  madvise(class_pool_start_[0], total_log_size_, MADV_HUGEPAGE);
#endif


  if (class_pool_start_[0] == nullptr || class_pool_start_[0] == MAP_FAILED) {
    ERROR_EXIT("mmap failed");
  }
  LOG("Log: pool_start %p total segments: %d  cleaners: %d\n", class_pool_start_[0],
      num_segments_, num_cleaners_);

  // all_segments_.resize(num_segments_ + 2 * num_cleaners_, nullptr);
  all_segments_.resize(num_segments_, nullptr);
  log_cleaners_.resize(num_cleaners_, nullptr);
  class_segments_.resize(num_class);
  cleaner_seg_sort_record.resize(num_workers_, -1);
  // class1_segments_.reserve((int)(num_class1_segments_ * 0.8));
  // class2_segments_.reserve((int)(num_class2_segments_ * 0.8));
  // class3_segments_.reserve((int)(num_class3_segments_ * 0.8));

  int i = 0, j = 0, num = 0;
  char * pool_start = class_pool_start_[0];
  for(int i = 1; i < num_class; i ++)
  {
    class_pool_start_[i] = class_pool_start_[i-1] +
                          num_class_segments_[i-1] * SEGMENT_SIZE[i-1];
  }
  for (i = 0, j = 0; i < num_segments_, j < num_cleaners_; i++) {
    // for class0
    if(j == 0)
    {
      if(i < num_class_segments_[j] - 1)
      {
        all_segments_[i] =
            new LogSegment(pool_start, SEGMENT_SIZE[j], j, i);
        free_segments_class[j].push(all_segments_[i]);
        pool_start += SEGMENT_SIZE[j];
        num_free_list_class[j] ++;
        assert(all_segments_[i]->is_segment_available());
      }
      else if(i == num_class_segments_[j] - 1)
      {
        all_segments_[i] =
            new LogSegment(pool_start, SEGMENT_SIZE[j], j, i);
        log_cleaners_[j] = new LogCleaner(db, j, this, all_segments_[i], j);
        pool_start += SEGMENT_SIZE[j];
        all_segments_[i]->set_reserved();
        assert(all_segments_[i]->is_segment_reserved());
        j++;
        num = 0;
        for(int n = 0; n < j; n ++) num += num_class_segments_[n];
      }
    }
    else
    {
      // for class123
      if(i < (num + num_class_segments_[j] * 0.8 - 1) )
      {
        all_segments_[i] =
            new LogSegment(pool_start, SEGMENT_SIZE[j], j, i);
        class_segments_[j].push_back(all_segments_[i]);
        pool_start += SEGMENT_SIZE[j];
        all_segments_[i]->set_touse();
        assert(all_segments_[i]->is_segment_touse());
      }
      else if(i < num + num_class_segments_[j] - 1)
      {
        all_segments_[i] =
            new LogSegment(pool_start, SEGMENT_SIZE[j], j, i);
        free_segments_class[j].push(all_segments_[i]);
        pool_start += SEGMENT_SIZE[j];
        num_free_list_class[j] ++;
        assert(all_segments_[i]->is_segment_available());
      }
      else if(i == num + num_class_segments_[j] - 1)
      {
        all_segments_[i] =
            new LogSegment(pool_start, SEGMENT_SIZE[j], j, i);
        log_cleaners_[j] = new LogCleaner(db, j, this, all_segments_[i], j);
        pool_start += SEGMENT_SIZE[j];
        all_segments_[i]->set_reserved();
        assert(all_segments_[i]->is_segment_reserved());
        j++;
        num = 0;
        for(int n = 0; n < j; n ++) num += num_class_segments_[n];
      }
    }
  }
  assert(j == num_class);

  printf("num_seg_class0 = %d, num_seg_class1 = %d (%ld), " 
         "num_seg_class2 = %d (%ld), num_seg_class3 = %d (%ld), "
         "num_seg = %d\nworkers = %d, cleaners = %d\n",
        num_class_segments_[0], num_class_segments_[1], class_segments_[1].size(), 
        num_class_segments_[2], class_segments_[2].size(), num_class_segments_[3],
        class_segments_[3].size(), num_segments_, num_workers_, num_cleaners_);


  for(int i = 1; i < num_class; i ++) 
  {
    db->db_num_class_segs[i] = get_num_class_segments_(i);
    db->change_seg_threshold_class[i] = SEGMENT_SIZE[i] / 3;
  }
  db->mark.resize(num_workers, false);
  db->first.resize(num_workers_, true);

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
  uint64_t class_[num_class] = {0};
  class_[0] = (num_class_segments_[0] - num_free_list_class[0]) * SEGMENT_SIZE[0];
  for(int i = 0, j = 1; j < num_class; i++)
  {
    if(i < class_segments_[j].size())
    {
      class_[j] += class_segments_[j][i]->get_offset();
    }
    else
    {
      class_[j] += log_cleaners_[j]->get_closed_list_sz() * SEGMENT_SIZE[j];
      i = 0;
      j++;
    }
  }

  for(int i = 0; i < num_class; i++)
  {
    printf("class%d seg usgae: %ld / %ld = %.3f%%\n",
      i, class_[i], num_class_segments_[i] * SEGMENT_SIZE[i],
      (double)class_[i]/(num_class_segments_[i] * SEGMENT_SIZE[i]));
  }
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
  munmap(class_pool_start_[0], total_log_size_);
  for(int i = 0; i < num_class; i++)
  {
    class_list_lock_[i].report();
  }
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
  LogSegment *ret = nullptr;
  // uint64_t waiting_time = 0;
  // TIMER_START(waiting_time);
  while (true) {
    if (num_free_list_class[class_] > 0) {
      std::lock_guard<SpinLock> guard(class_list_lock_[class_]);
      if (!free_segments_class[class_].empty()) {
        ret = free_segments_class[class_].front();
        free_segments_class[class_].pop();
        --num_free_list_class[class_];
      }
    } else {
      if (num_cleaners_ == 0) {
        ERROR_EXIT("No free segments and no cleaners");
      }
      usleep(1);
    }
    if (ret) {
      break;
    }
  }
  // TIMER_STOP(waiting_time);

  COUNTER_ADD_LOGGING(num_new_segment_, 1);
  // not store shortcuts for hot segment
  ret->StartUsing(true);

  // UpdateCleanThreshold(class_);
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
  assert(segment_id < num_segments_);
  return all_segments_[segment_id];
}

int LogStructured::GetSegmentID(const char *addr) {
  assert(addr >= class_pool_start_[0]);
  int seg_id, i, pre = 0;

  for(i = 1; i < num_class; i++)
  {
    if(addr < class_pool_start_[i])
    {
      for(int j = 0; j < i - 1; j++)
      {
        pre += num_class_segments_[j];
      }
      seg_id = pre + (addr - class_pool_start_[i-1]) / SEGMENT_SIZE[i-1];
      break;
    }
  }
  if(i == num_class)
  {
    for(int j = 0; j < i - 1; j++)
    {
      pre += num_class_segments_[j];
    }
    seg_id = pre + (addr - class_pool_start_[num_class-1]) / SEGMENT_SIZE[num_class-1];
  } 

  assert(seg_id < num_segments_);
  return seg_id;
}

int LogStructured::GetSegmentCleanerID(const char *addr) {
  return GetSegmentID(addr) % num_cleaners_;
}

void LogStructured::AddClosedSegment(LogSegment *segment, int class_) {
  // int cleaner_id = GetSegmentCleanerID(segment->get_segment_start());
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
  for (int i = 0; i < num_cleaners_; i++) {
    int clean_seg_count = log_cleaners_[i]->clean_seg_count_ -
                          log_cleaners_[i]->clean_seg_count_before_;
    uint64_t clean_time_ns = log_cleaners_[i]->clean_time_ns_ -
                             log_cleaners_[i]->clean_time_ns_before_;
    double tp = 1.0 * clean_seg_count * SEGMENT_SIZE[i] * 1e9 /
                clean_time_ns;
    total_tp += tp;
  }
  return total_tp;
}

void LogStructured::UpdateCleanThreshold(int class_) {
  int cnt = alloc_counter_.fetch_add(1);

  if (!FS_flag_.test_and_set()) {
    int num_free = num_free_list_class[class_].load(std::memory_order_relaxed);
    uint64_t num_seg = num_class_segments_[class_] - class_segments_[class_].size();
    double step = num_seg / 200.;  // 0.5%
    double thresh_free = num_seg * clean_threshold_[class_] / 100.;

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
          if (num_free > thresh_free - step && clean_threshold_[class_] < 10) {
            // if num_free is much less than thresh, doesn't need to change
            // thresh
            ++clean_threshold_[class_];
          }
          free_status_ = FS_Insufficient;
        } else if (num_free > thresh_free &&
                   thresh_free - step > num_limit_free_segments_ &&
                   clean_threshold_[class_] > 1 && cnt > num_workers_ * 4) {
          alloc_counter_ = 0;
          --clean_threshold_[class_];
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
