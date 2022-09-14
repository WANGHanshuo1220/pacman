#pragma once

#include <vector>
#include <list>
#include <memory>
#include <thread>
#include <unordered_set>
#include <unordered_map>

#include "slice.h"
#include "config.h"
#include "util/lock.h"

static constexpr size_t HOT_NUM = 256 * 1024;
static constexpr int RECORD_BATCH_CNT = 4096;
static constexpr size_t RECORD_BUFFER_SIZE = 16 * 1024;

struct RecordEntry {
  uint64_t key;
  int64_t cnt;

  bool operator>(const RecordEntry &other) const {
    return cnt > other.cnt;
  }
};

struct alignas(CACHE_LINE_SIZE) UpdateKeyRecord {
  int hit_cnt[4] = {0};
  int total_cnt = 0;
  SpinLock lock;
  std::list<std::vector<uint64_t> > records_list;
  std::vector<uint64_t> records;

  UpdateKeyRecord() : lock("") {
    records.reserve(RECORD_BUFFER_SIZE);
  }
};

class DB;
class HotKeySet {
 public:
  explicit HotKeySet(DB *db);
  ~HotKeySet();

  void Record(const Slice &key, int worker_id, int class_);
  void BeginUpdateHotKeySet();
  int Exist(const Slice &key);
  // uint64_t get_set_sz() 
  // { 
  //   uint32_t re = 0;
  //   if(current_set_class1)
  //     re +=  (*current_set_class1).size(); 
  //   if(current_set_class2)
  //     re +=  (*current_set_class2).size(); 
  //   if(current_set_class3)
  //     re +=  (*current_set_class3).size(); 
  //   return re;
  // }
  uint64_t Record_c = 0;

 private:
  DB *db_;
  std::unordered_set<uint64_t> *current_set_class1 = nullptr;
  std::unordered_set<uint64_t> *current_set_class2 = nullptr;
  std::unordered_set<uint64_t> *current_set_class3 = nullptr;
  std::unique_ptr<UpdateKeyRecord[]> update_record_;
  std::thread update_hot_set_thread_;
  std::atomic_flag update_schedule_flag_{ATOMIC_FLAG_INIT};
  volatile bool need_record_ = false;
  volatile bool need_count_hit_ = true;
  std::atomic_bool stop_flag_{false};

  void UpdateHotSet();
};
