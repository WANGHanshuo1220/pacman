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
LogStructured::LogStructured(std::string db_path[], size_t log_size, DB *db,
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
    
    uint64_t sz = 0;
    for(int i = 1; i < num_class; i++)
    {
      num_class_segments_[i] = 
        class_sz[i] * 1024 * 1024 / SEGMENT_SIZE[i];
      if(num_class_segments_[i] % 4 != 0)
        num_class_segments_[i] = (num_class_segments_[i] / 4 + 1) * 4;
      sz += num_class_segments_[i] * SEGMENT_SIZE[i];
    }

    num_class_segments_[0] = 
      ((total_log_size_ - sz) / SEGMENT_SIZE[0]) / num_channel * num_channel;

    for(int i = 0; i < num_class; i++)
    {
      num_segments_ += num_class_segments_[i];
    }
#ifdef LOG_PERSISTENT
  std::string log_pool_path[num_channel];
  int log_pool_fd[num_channel];
  for(int i = 0; i < num_channel; i++)
  {
    log_pool_path[i] = db_path[i] + "/log_pool";
    log_pool_fd[i] = open(log_pool_path[i].c_str(), O_CREAT | O_RDWR, 0644);
    if (log_pool_fd[i] < 0) {
      ERROR_EXIT("open file failed");
    }
  }

  uint64_t ch_sz[num_channel] = {
                num_class_segments_[0] / num_channel * SEGMENT_SIZE[0] + 
                num_class_segments_[1] / 2 * SEGMENT_SIZE[1] + 
                num_class_segments_[2] / 2 * SEGMENT_SIZE[2],
                num_class_segments_[0] / num_channel * SEGMENT_SIZE[0] + 
                num_class_segments_[1] / 2 * SEGMENT_SIZE[1] + 
                num_class_segments_[2] / 2 * SEGMENT_SIZE[2],
                num_class_segments_[0] / num_channel * SEGMENT_SIZE[0],
                num_class_segments_[0] / num_channel * SEGMENT_SIZE[0]
                };

  for(int i = 0; i < num_channel; i++)
  {
    if (fallocate(log_pool_fd[i], 0, 0, ch_sz[i]) != 0) {
      ERROR_EXIT("fallocate file failed %d", i);
    }
    pool_start_[i] = (char *)mmap(NULL, ch_sz[i], PROT_READ | PROT_WRITE,
                               MAP_SHARED, log_pool_fd[i], 0);
    pool_end_[i] = pool_start_[i] + ch_sz[i];
    close(log_pool_fd[i]);
    if (pool_start_ == nullptr || pool_start_ == MAP_FAILED) {
      ERROR_EXIT("mmap failed");
    }
  }
#endif

  if (pool_start_[0] == nullptr || pool_start_[0] == MAP_FAILED) {
    ERROR_EXIT("mmap failed");
  }
  LOG("Log: pool_start %p total segments: %d  cleaners: %d\n", pool_start_[0],
      num_segments_, num_cleaners_);

  all_segments_.resize(num_segments_, nullptr);
  log_cleaners_.resize(num_cleaners_, nullptr);
  class_segments_.resize(num_class);

  for(int i = 0; i < num_channel; i++)
  {
    class_pool_start_[0][i] = pool_start_[i];
    class_pool_end_[0][i] = class_pool_start_[0][i] + 
                          num_class_segments_[0] / num_channel * SEGMENT_SIZE[0];
  }

  for(int i = 1; i < num_class; i++)
  {
    for(int j = 0; j < 2; j++)
    {
      class_pool_start_[i][j] = class_pool_end_[i-1][j];
      class_pool_end_[i][j] = class_pool_start_[i][j] + 
                              num_class_segments_[i] / 2 * SEGMENT_SIZE[i];
    }
  }

  for(int i = 1; i < num_class; i++)
  {
    for(int j = 2; j < num_channel; j++)
    {
      class_pool_end_[i][j] = class_pool_start_[i][j] = class_pool_end_[0][j];
    }
  }

  char *pool_start[4] = {pool_start_[0], pool_start_[1], pool_start_[2], pool_start_[3]};

  std::vector<int> num_class_IGC;
  num_class_IGC.resize(num_class);
  for(int i = 1; i < num_class; i++)
  {
    num_class_IGC[i] = num_class_segments_[i] * 0.8;
    num_class_IGC[i] = (num_class_IGC[i] / num_workers_ + 1) * num_workers_;
    if(num_class_IGC[i]/num_class_segments_[i] > 0.9)
      num_class_IGC[i] = (num_class_IGC[i] / num_workers_ ) * num_workers_;
  }

  std::vector<LogSegment*> reserved_helper_segments;
  int i = 0, j = 0, num = 0, cleaner_c = 0;
  int channel = 0;
  for (i = 0, j = 0; i < num_segments_, j < num_class; i++) {
    // for class0
    if(j == 0)
    {
      if(i < num_class_segments_[0] - num_cleaners_)
      {
        all_segments_[i] =
            new LogSegment(pool_start[channel], SEGMENT_SIZE[0], 0, i, channel);
        free_segments_class[0].push_back(all_segments_[i]);
        pool_start[channel] += SEGMENT_SIZE[0];
        num_free_list_class[0] ++;
        channel++;
        channel = channel % num_channel;
      }
      else if(i < num_class_segments_[0] - (num_class - 1) &&
              i >= num_class_segments_[0] - num_cleaners_)
      {
        all_segments_[i] =
            new LogSegment(pool_start[channel], SEGMENT_SIZE[0], 0, i, channel);
        log_cleaners_[cleaner_c] = new LogCleaner(db, cleaner_c, this, all_segments_[i], 0, nullptr);
        cleaner_c ++;
        pool_start[channel] += SEGMENT_SIZE[0];
        channel++;
        channel = channel % num_channel;
      }
      else if(i >= num_class_segments_[0] - (num_class-1) &&
              i < num_class_segments_[0])
      {
        all_segments_[i] =
            new LogSegment(pool_start[channel], SEGMENT_SIZE[0], 0, i, channel);
        pool_start[channel] += SEGMENT_SIZE[0];
        reserved_helper_segments.push_back(all_segments_[i]);
        channel++;
        channel = channel % num_channel;
        if(i == num_class_segments_[0]-1)
        {
          j ++;
          channel = 0;
        }
      }
    }
    else
    {
      // for class123
      num = 0;
      for(int n = 0; n < j; n ++) num += num_class_segments_[n];

      if(i < (num + num_class_IGC[j]) && i >= num)
      {
        all_segments_[i] =
            new LogSegment(pool_start[channel], SEGMENT_SIZE[j], j, i, channel);
        class_segments_[j].push_back(all_segments_[i]);
        pool_start[channel] += SEGMENT_SIZE[j];
        all_segments_[i]->set_using();
        channel++;
        channel = channel % 2;
      }
      else if(i < num + num_class_segments_[j] - 1 &&
              i >= (num + num_class_IGC[j]) )
      {
        all_segments_[i] =
            new LogSegment(pool_start[channel], SEGMENT_SIZE[j], j, i, channel);
        free_segments_class[j].push_back(all_segments_[i]);
        pool_start[channel] += SEGMENT_SIZE[j];
        num_free_list_class[j] ++;
        channel ++;
        channel = channel % 2;
      }
      else if(i == num + num_class_segments_[j] - 1)
      {
        all_segments_[i] =
            new LogSegment(pool_start[channel], SEGMENT_SIZE[j], j, i, channel);
        log_cleaners_[cleaner_c] = new LogCleaner(db, cleaner_c, this, all_segments_[i], j, reserved_helper_segments[j-1]);
        cleaner_c ++;
        pool_start[channel] += SEGMENT_SIZE[j];
        j++;
        channel = 0;
      }
    }
  }

  printf("num_seg_class0 = %d; ", num_class_segments_[0]);
  for(int a = 1; a < num_class; a++)
  {
    printf("num_seg_class%d = %d (%ld), free_seg_list = %d; ",
      a, num_class_segments_[a], class_segments_[a].size(),
      num_free_list_class[a].load());
  }
  printf("num_segs = %d\n", num_segments_);
  printf("workers = %d, cleaners = %d\n", num_workers, num_cleaners);

  // for(int i = 0; i < num_channel; i ++)
  // {
  //   printf("ch %d : ", i);
  //   for(int j = 0; j < num_class; j++)
  //   {
  //     printf("%p ~ %p ", class_pool_start_[j][i], class_pool_end_[j][i]);
  //   }
  //   printf("\n");
  // }

  for(int i = 1; i < num_class; i ++) 
  {
    db->db_num_class_segs[i] = get_num_class_segments_(i);
    db->change_seg_threshold_class[i] = (SEGMENT_SIZE[i] - HEADER_ALIGN_SIZE) / 2;
  }

  for (int j = 0; j < num_cleaners_; j++) {
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

  uint64_t c = 0;
  uint64_t d = 0;
  for(int i = 0; i < num_cleaners_; i++)
  {
    // printf("cleaner%d: flush_times = %ld\tGC_times = %d\tGC_help_times = %d, quick_c = %d\n", 
    //   i, log_cleaners_[i]->flush_times, log_cleaners_[i]->GC_times,
    //   log_cleaners_[i]->GC_times_help, log_cleaners_[i]->quick_c);
    c += log_cleaners_[i]->flush_times;
    d += log_cleaners_[i]->GC_times /
         (SEGMENT_SIZE[0] / SEGMENT_SIZE[log_cleaners_[i]->get_class()]);
    d += log_cleaners_[i]->GC_times_help;
  }
  printf("total flush_times = %ld (%.2f)\n", c, (float)c/1000000);
  printf("total gc_times    = %.2f\n", (float)d);

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
  uint64_t ch_sz[num_channel] = {
                num_class_segments_[0] / num_channel * SEGMENT_SIZE[0] + 
                num_class_segments_[1] / 2 * SEGMENT_SIZE[1] + 
                num_class_segments_[2] / 2 * SEGMENT_SIZE[2],
                num_class_segments_[0] / num_channel * SEGMENT_SIZE[0] + 
                num_class_segments_[1] / 2 * SEGMENT_SIZE[1] + 
                num_class_segments_[2] / 2 * SEGMENT_SIZE[2],
                num_class_segments_[0] / num_channel * SEGMENT_SIZE[0],
                num_class_segments_[0] / num_channel * SEGMENT_SIZE[0]
                };
  for(int i = 0; i < num_channel; i++)
  {
    munmap(pool_start_[i], ch_sz[i]);
  }
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

LogSegment *LogStructured::NewSegment(int class_t) {
  static int c = 0;
  LogSegment *ret = nullptr;
  // uint64_t waiting_time = 0;
  // TIMER_START(waiting_time);
  int class_t_ = class_t;
  if(class_t_ == -1) class_t_ = 0;
  while (true) {
    if (num_free_list_class[class_t_] > 0) {
      std::lock_guard<SpinLock> guard(class_list_lock_[class_t_]);
      if (!free_segments_class[class_t_].empty()) {
        // if(class_t == 0)
        // {
        //   for(auto it = free_segments_class[0].begin(); 
        //       it != free_segments_class[0].end(); it++)
        //   {
        //     if((*it)->get_channel() < num_channel/2)
        //     {
        //       ret = (*it);
        //       free_segments_class[0].erase(it);
        //       --num_free_list_class[0];
        //       goto out;
        //     }
        //   }
        // }
        ret = free_segments_class[class_t_].front();
        free_segments_class[class_t_].pop_front();
        --num_free_list_class[class_t_];
        // if(class_t == 2) printf("new success\n");
      }
    } else {
      // if(class_t == 2) printf("new failed %d, %ld\n", c++, free_segments_class[class_t_].size());
      if (num_cleaners_ == 0) {
        ERROR_EXIT("No free segments and no cleaners");
      }
      usleep(1);
    }
    if (ret) {
      break;
    }
  }

// out:
  // not store shortcuts for hot segment
  bool has_sc = (class_t_ > 0) ? false : true;
  bool hot = (class_t == -1) ? false : true;
  ret->StartUsing(hot, has_sc);

  UpdateCleanThreshold(class_t_);
  return ret;
}

void LogStructured::FreezeSegment(LogSegment *old_segment, int class_t) {
  if (old_segment != nullptr && num_cleaners_ > 0) {
    old_segment->Close();
    AddClosedSegment(old_segment, class_t);
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
  int seg_id = 0, i, j, k;

  for(i = 0; i < num_class; i++)
  {
    for(j = 0; j < num_channel; j++)
    {
      if(addr >= class_pool_start_[i][j] && 
         addr < class_pool_end_[i][j])
      {
        seg_id = prop[i] * ((addr - class_pool_start_[i][j]) / SEGMENT_SIZE[i]) + j;
        for(k = 0; k < i; k++)
        {
          seg_id += num_class_segments_[k];
        }
        goto out;
      }
    }
  }

out:
  if(addr < GetSegment(seg_id)->get_segment_start() ||
     addr > GetSegment(seg_id)->get_end())
  {
    printf("%d\taddr  = %p\n", seg_id, addr);
    printf("%d\tstart = %p\n", seg_id, GetSegment(seg_id)->get_segment_start());
    printf("%d\tend   = %p\n", seg_id, GetSegment(seg_id)->get_end());
    printf("%d\tid    = %ld\n", seg_id, GetSegment(seg_id)->get_seg_id());
    printf("%d\tclass = %d\n", seg_id, GetSegment(seg_id)->get_class());
    printf("%d\ti,j,k = %d %d %d\n", seg_id, i, j, k);
  }
  assert(GetSegment(seg_id)->get_segment_start() <= addr);
  assert(GetSegment(seg_id)->get_end() > addr);
  return seg_id;
}

int LogStructured::GetSegmentCleanerID(const char *addr) {
  return GetSegmentID(addr) % num_cleaners_;
}

void LogStructured::AddClosedSegment(LogSegment *segment, int class_t) {
  if(class_t > 0)
  {
    for(int i = 1; i < num_class; i++)
    {
      if(class_t == num_class-i) 
        log_cleaners_[num_cleaners_-i]->AddClosedSegment(segment, class_t);
    }
  }
  else
  {
    int cleaner_id = GetSegmentCleanerID(segment->get_segment_start());
    log_cleaners_[cleaner_id]->AddClosedSegment(segment, class_t);
  }
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
  // stop compaction first.
  stop_flag_.store(true, std::memory_order_release);
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      log_cleaners_[i]->StopThread();
    }
  }
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      delete log_cleaners_[i];
      log_cleaners_[i] = nullptr;
    }
  }

  uint64_t free_segs = 0, num_free_segs = 0;
  for(int i = 0; i < num_class; i++)
  {
    free_segs += free_segments_class[i].size();
    num_free_segs += num_free_list_class[i];
  }
  assert(free_segs == num_free_segs);
  printf("before recovery: %ld free segments\n", free_segs);
  
  // clean segments, simulate a clean restart.
  for(int i = 0; i < num_class; i++)
  {
    while (!free_segments_class[i].empty()) {
      free_segments_class[i].pop_back();
    }
    num_free_list_class[i] = 0;
  }

  for (int i = 0; i < num_segments_; i++) {
    if (all_segments_[i]) {
      delete all_segments_[i];
    }
  }
  all_segments_.clear();
  all_segments_.resize(num_segments_, nullptr);
  
  for(int i = 0; i < num_class; i++)
  {
    class_segments_[i].clear();
  }

  recovery_counter_ = 0;
  
  // start recovery
  log_cleaners_[0] = new LogCleaner(db, 0, this, nullptr, 0, nullptr);
  for (int i = 1; i < num_cleaners_; i++) {
    log_cleaners_[i] = new LogCleaner(db, i, this, nullptr, i-1, nullptr);
  }

  uint64_t rec_time = 0;
  TIMER_START(rec_time);
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->StartRecoverySegments();
  }

  // wait for recovery over
  {
    std::unique_lock<std::mutex> lk(rec_mu_);
    while (recovery_counter_ < num_cleaners_) {
      rec_cv_.wait(lk);
    }
  }
  TIMER_STOP(rec_time);
  uint64_t free_segs_ = 0, num_free_segs_ = 0;
  for(int i = 0; i < num_class; i++)
  {
    free_segs_ += free_segments_class[i].size();
    num_free_segs_ += num_free_list_class[i];
  }
  printf("after recovery: %ld free segments\n", free_segs_);
  assert(free_segs_ == num_free_segs_);
  printf("recovery segments time %lu us\n", rec_time / 1000);
}

void LogStructured::RecoveryInfo(DB *db) {
  if (recovery_counter_ != num_cleaners_) {
    ERROR_EXIT("Should recovery segments first");
  }
  recovery_counter_ = 0;
  uint64_t rec_time = 0;
  TIMER_START(rec_time);
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->StartRecoveryInfo();
  }

  // wait for recovery over
  {
    std::unique_lock<std::mutex> lk(rec_mu_);
    while (recovery_counter_ < num_cleaners_) {
      rec_cv_.wait(lk);
    }
  }
  TIMER_STOP(rec_time);
  printf("recovery info time %lu us\n", rec_time / 1000);
}


void LogStructured::RecoveryAll(DB *db) {
  // stop compaction first.
  stop_flag_.store(true, std::memory_order_release);
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      log_cleaners_[i]->StopThread();
    }
  }
  for (int i = 0; i < num_cleaners_; i++) {
    if (log_cleaners_[i]) {
      delete log_cleaners_[i];
      log_cleaners_[i] = nullptr;
    }
  }

  uint64_t free_segs = 0, num_free_segs = 0;
  for(int i = 0; i < num_class; i++)
  {
    free_segs += free_segments_class[i].size();
    num_free_segs += num_free_list_class[i];
  }
  assert(free_segs == num_free_segs);
  printf("before recovery: %ld free segments\n", free_segs);

  // clear old segments
  for(int i = 0; i < num_class; i++)
  {
    while (!free_segments_class[i].empty()) {
      free_segments_class[i].pop_back();
    }
    num_free_list_class[i] = 0;
  }

  for (int i = 0; i < num_segments_; i++) {
    if (all_segments_[i]) {
      delete all_segments_[i];
    }
  }
  all_segments_.clear();
  all_segments_.resize(num_segments_, nullptr);  

  for(int i = 0; i < num_class; i++)
  {
    class_segments_[i].clear();
  }

  // delete old index
#ifndef IDX_PERSISTENT
  db->NewIndexForRecoveryTest();
#endif

  recovery_counter_ = 0;
  
  // start recovery
  log_cleaners_[0] = new LogCleaner(db, 0, this, nullptr, 0, nullptr);
  for (int i = 1; i < num_cleaners_; i++) {
    log_cleaners_[i] = new LogCleaner(db, i, this, nullptr, i-1, nullptr);
  }

  uint64_t rec_time = 0;
  TIMER_START(rec_time);
  for (int i = 0; i < num_cleaners_; i++) {
    log_cleaners_[i]->StartRecoveryAll();
  }

  // wait for recovery over
  {
    std::unique_lock<std::mutex> lk(rec_mu_);
    while (recovery_counter_ < num_cleaners_) {
      rec_cv_.wait(lk);
    }
  }
  TIMER_STOP(rec_time);
  uint64_t free_segs_ = 0, num_free_segs_ = 0;
  for(int i = 0; i < num_class; i++)
  {
    free_segs_ += free_segments_class[i].size();
    num_free_segs_ += num_free_list_class[i];
  }
  printf("after recovery: %ld free segments\n", free_segs_);
  assert(free_segs_ == num_free_segs_);
  printf("recovery all time %lu us\n", rec_time / 1000);
}
