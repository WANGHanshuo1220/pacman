#include "hotkeyset.h"
#include "db.h"

#include <queue>
#include <mutex>

HotKeySet::HotKeySet(DB *db) : db_(db) {
  Record_c = 0;
  for(int i = 0; i < num_class-1; i++)
  {
    current_set_class[i] = nullptr;
  }
  update_record_ = std::make_unique<UpdateKeyRecord[]>(db_->num_workers_);
}

HotKeySet::~HotKeySet() {
  need_record_ = false;
  stop_flag_.store(true);
  if (update_hot_set_thread_.joinable()) {
    update_hot_set_thread_.join();
  }

  for(int i = 0; i < num_class-1; i++)
  {
    if (current_set_class[i]) {
      delete current_set_class[i];
    }
  }
}

void HotKeySet::Record(const Slice &key, int worker_id, int class_) {
  // printf("in Record %ld\n", Record_c);
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
    if(class_ != 0) 
    {
      record.hit_cnt[class_] ++;
    }
    ++record.total_cnt;
    if (record.total_cnt == RECORD_BATCH_CNT) { // sampling rate
      uint32_t total_hit = 0;
      for(int i = 1; i < num_class; i++) total_hit += record.hit_cnt[i];
      // printf("hit ratio = %.1lf%%\n", 100. * record.hit_cnt / record.total_cnt);
      if (     total_hit    < RECORD_BATCH_CNT  * 0.7
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
          HOT_NUM = total_num_key * 0.40;
          BeginUpdateHotKeySet();
        }
      }
      record.hit_cnt[1] = record.hit_cnt[2] = record.hit_cnt[3] = record.total_cnt = 0;
    }
  }
  // Record_c++;
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
  if(!current_set_class[0])
  {
    return 0;
  }
  else
  {
    for(int i = num_class-2 ; i >= 0; i--)
    {
      if(current_set_class[i]->find(i_key) != current_set_class[i]->end())
        return (i+1);
    }
  }
  
  return 0;
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

  std::unordered_set<uint64_t> *old_set_class1 = current_set_class[0];
  std::unordered_set<uint64_t> *old_set_class2 = current_set_class[1];
  std::unordered_set<uint64_t> *old_set_class3 = current_set_class[2];
  std::unordered_set<uint64_t> *new_set_class1 = nullptr;
  std::unordered_set<uint64_t> *new_set_class2 = nullptr;
  std::unordered_set<uint64_t> *new_set_class3 = nullptr;
  int sz = topK.size();
  int a1, a2, a3;
  a3 = topK.size() / 40;
  a2 = topK.size() / 5;
  a1 = topK.size() - a3 - a2;
  // printf("min = %ld\n", topK.top().cnt);
  if (!topK.empty()) {
    if (max_cnt > 3 * topK.top().cnt) {
      new_set_class1 = new std::unordered_set<uint64_t>(a1);
      new_set_class2 = new std::unordered_set<uint64_t>(a2);
      new_set_class3 = new std::unordered_set<uint64_t>(a3);
      for(int i = 0; !topK.empty(); i ++) {
        // if(topK.size() == 1) printf("max = %ld\n", topK.top().cnt);
        if(i < a1) new_set_class1->insert(topK.top().key);
        else if(i < a1 + a2) new_set_class2->insert(topK.top().key);
        else new_set_class3->insert(topK.top().key);
        topK.pop();
      }
      LOG("new class1 set size %lu", new_set_class1->size());
      LOG("new class2 set size %lu", new_set_class2->size());
      LOG("new class3 set size %lu", new_set_class3->size());
    }
  }

  current_set_class[0] = new_set_class1;
  current_set_class[1] = new_set_class2;
  current_set_class[2] = new_set_class3;
  db_->thread_status_.rcu_barrier();
  for (int i = 0; i < db_->num_workers_; i++) {
    // need_record_ is false, other threads cannot operate on records
    update_record_[i].records.clear();
    update_record_[i].records_list.clear();
    for(int j = 0; j < num_class; j++) update_record_[i].hit_cnt[j] = 0; 
    update_record_[i].total_cnt = 0;
  }
  if (old_set_class1) {
    delete old_set_class1;
  }
  if (old_set_class2) {
    delete old_set_class2;
  }
  if (old_set_class3) {
    delete old_set_class3;
  }

  update_schedule_flag_.clear(std::memory_order_relaxed);
  need_count_hit_ = true;
}
