#include "hotkeyset.h"
#include "db.h"

#include <queue>
#include <mutex>

HotKeySet::HotKeySet(DB *db) : db_(db) {
  current_set_class.resize(num_class, nullptr);
  update_record_ = std::make_unique<UpdateKeyRecord[]>(db_->num_workers_);
}

HotKeySet::~HotKeySet() {
  need_record_ = false;
  stop_flag_.store(true);
  if (update_hot_set_thread_.joinable()) {
    update_hot_set_thread_.join();
  }

  for(int i = 0; i < num_class; i++)
  {
    if (current_set_class[i]) {
      delete current_set_class[i];
    }
  }
}

void HotKeySet::Record(const Slice &key, int worker_id, int class_t) {
  UpdateKeyRecord &record = update_record_[worker_id];
  if (need_record_) {
    uint64_t i_key = *(uint64_t *)key.data();
    record.records.push_back(i_key);
    if (record.records.size() >= RECORD_BUFFER_SIZE) {
      std::lock_guard<SpinLock> lock(record.lock);
      record.records_list.push_back(std::move(record.records));
      record.records.reserve(RECORD_BUFFER_SIZE);
    }
  } else if (need_count_hit_) {
    if(class_t != -1) 
    {
      record.hit_cnt ++;
    }
    ++record.total_cnt;
    if (record.total_cnt == RECORD_BATCH_CNT) { // sampling rate
      // printf("hit ratio = %.1lf%%\n", 100. * record.hit_cnt / record.total_cnt);
      if (     record.hit_cnt  < RECORD_BATCH_CNT  * 0.5
          // record.hit_cnt[3] < record.hit_cnt[2] * 1.0 ||
          // record.hit_cnt[2] < record.hit_cnt[1] * 1.0 
         ) { /* means many keys that has been accessed 
              * frequently in a sampling period have not 
              * been add in to current_set_, so current_set
              * need to be updated. 
              */
        // LOG("hit ratio = %.1lf%%", 100. * record.hit_cnt / record.total_cnt);
        if (!update_schedule_flag_.test_and_set()) {
          uint64_t total_num_key = db_->index_->get_num_key();
          HOT_NUM = total_num_key * 0.01;
          // HOT_NUM = 128 * 1024;
          BeginUpdateHotKeySet();
        }
      }
      record.hit_cnt = record.total_cnt = 0;
    }
  }
}

void HotKeySet::BeginUpdateHotKeySet() {
  need_record_ = true;
  need_count_hit_ = false;
  if (update_hot_set_thread_.joinable()) {
    update_hot_set_thread_.join();
  }
  update_hot_set_thread_ = std::thread(&HotKeySet::UpdateHotSet, this);
}

int HotKeySet::Exist(const Slice &key) {
  uint64_t i_key = *(uint64_t *)key.data();
  if(current_set_class[0] == nullptr)
  {
    return -1;
  }
  else
  {
    for(int i = num_class-1; i >= 0; i--)
    {
      if(current_set_class[i]->find(i_key) != current_set_class[i]->end())
        return i;
    }
  }
  
  return -1;
}

void HotKeySet::UpdateHotSet() {
  // bind_core_on_numa(db_->num_workers_);

  std::unordered_map<uint64_t, int> count;
  uint64_t update_cnt = 0;
  while (!stop_flag_.load(std::memory_order_relaxed)) {
    if (count.size() > HOT_NUM * 4 || update_cnt > HOT_NUM * 16) {
      break;
    }
    std::list<std::vector<uint64_t>> list;
    for (int i = 0; i < db_->num_workers_; i++) {
      std::lock_guard<SpinLock> lock(update_record_[i].lock);
      list.splice(list.end(), update_record_[i].records_list);
    }
    for (auto it = list.begin(); it != list.end(); it++) {
      for (int i = 0; i < it->size(); i++) {
        ++count[it->at(i)];
      }
    }
    update_cnt += list.size() * RECORD_BUFFER_SIZE;
  }

  need_record_ = false;

  std::priority_queue<RecordEntry, std::vector<RecordEntry>,
                      std::greater<RecordEntry>>
      topK;
  int max_cnt = 0;
  for (auto it = count.begin(); it != count.end(); it++) {
    if (it->second > 1) {
      max_cnt = std::max(max_cnt, it->second);
      if (topK.size() < HOT_NUM) {
        topK.push({it->first, it->second});
      } else if (it->second > topK.top().cnt) {
        topK.pop();
        topK.push({it->first, it->second});
      }
    }
  }

  std::vector<std::unordered_set<uint64_t>*>old_set_class;
  std::vector<std::unordered_set<uint64_t>*>new_set_class;
  old_set_class.resize(num_class, nullptr);
  new_set_class.resize(num_class, nullptr);
  for(int i = 0; i < num_class; i++)
  {
    old_set_class[i] = current_set_class[i];
    new_set_class[i] = nullptr;
  }

  int sz = topK.size();
  int a[num_class];
  a[2] = topK.size() / 40;
  a[1] = topK.size() / 10;
  a[0] = topK.size() - a[1] - a[2];
  for(int i = 0; i < num_class; i++)
  {
    new_set_class[i] = new std::unordered_set<uint64_t>();
  }
  if (!topK.empty()) {
    if (max_cnt > 3 * topK.top().cnt) {
      for(int i = 0; !topK.empty(); i ++) {
        if(i < a[0]) new_set_class[0]->insert(topK.top().key);
        else if(i < a[0] + a[1] && i >= a[0]) new_set_class[1]->insert(topK.top().key);
        else new_set_class[2]->insert(topK.top().key);
        topK.pop();
      }
      for(int i = 0; i < num_class; i++) 
        LOG("new class%d set size %lu", i, new_set_class[i]->size());
    }
  }

  for(int i = 0; i < num_class; i++)
  {
    current_set_class[i] = new_set_class[i];
  }
  db_->thread_status_.rcu_barrier();
  for (int i = 0; i < db_->num_workers_; i++) {
    // need_record_ is false, other threads cannot operate on records
    update_record_[i].records.clear();
    update_record_[i].records_list.clear();
    update_record_[i].total_cnt = 0;
  }

  for(int i = 0; i < num_class; i++)
  {
    if (old_set_class[i]) {
      delete old_set_class[i];
    }
  }

  update_schedule_flag_.clear(std::memory_order_relaxed);
  need_count_hit_ = true;
}
