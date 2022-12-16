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
#include "db_common.h"

typedef std::unordered_map<KeyType, struct hash_sc*> Map;
enum {NotChanging, Changing, ChangingDone};

static size_t HOT_NUM = 256 * 1024;
static constexpr int RECORD_BATCH_CNT = 4096;
static constexpr size_t RECORD_BUFFER_SIZE = 16 * 1024;

struct RecordEntry {
  uint64_t key = 0;
  int64_t cnt = 0;

  bool operator>(const RecordEntry &other) const {
    return cnt > other.cnt;
  }
};

struct alignas(CACHE_LINE_SIZE) UpdateKeyRecord {
  int hit_cnt = 0;
  int class2_hit_cnt = 0;
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
  uint64_t get_set_sz(int class_) 
  { 
    if(current_set_class[class_])
      return (*current_set_class[class_]).size(); 
    else
      return 0;
  }
  bool has_hot_set() { return has_hot_set_.load(); }
#ifdef HOT_SC
  Map *old_hot_sc = nullptr;
  Map *new_hot_sc = nullptr;
  bool is_changing() { return changing_status.load() == Changing; }
  bool not_changing() { return changing_status.load() == NotChanging; }
  bool changing_done() { return changing_status.load() == ChangingDone; }
#endif

 private:
  DB *db_;
  std::vector<std::unordered_set<uint64_t>*>current_set_class;
  std::unique_ptr<UpdateKeyRecord[]> update_record_;
  std::thread update_hot_set_thread_;
  std::atomic_flag update_schedule_flag_{ATOMIC_FLAG_INIT};
  volatile bool need_record_ = false;
  volatile bool need_count_hit_ = true;
  std::atomic_bool stop_flag_{false};
  std::atomic_bool has_hot_set_{false};
#ifdef HOT_SC
  std::atomic<int> changing_status = NotChanging;
#endif

  void UpdateHotSet();
};
