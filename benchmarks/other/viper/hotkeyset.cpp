#include "hotkeyset.h"

#include <queue>
#include <mutex>
#include <unistd.h>

HotKeySet::HotKeySet(int num_workers) : num_workers_(num_workers) {
  current_set_ = nullptr;
  update_record_ = std::make_unique<UpdateKeyRecord[]>(num_workers_);
}

HotKeySet::~HotKeySet() {
  need_record_ = false;
  stop_flag_.store(true);
  if (update_hot_set_thread_.joinable()) {
    update_hot_set_thread_.join();
  }
  if (current_set_) {
    delete current_set_;
  }
}

void HotKeySet::Record(const Slice &key, int worker_id, bool hit) {
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
    record.hit_cnt += hit;
    ++record.total_cnt;
    if (record.total_cnt == RECORD_BATCH_CNT) {
      if (record.hit_cnt < RECORD_BATCH_CNT * 0.5) {
        // LOG("hit ratio = %.1lf%%", 100. * record.hit_cnt / record.total_cnt);
        if (!update_schedule_flag_.test_and_set()) {
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

bool HotKeySet::Exist(const Slice &key) {
  uint64_t i_key = *(uint64_t *)key.data();
  return current_set_ && current_set_->find(i_key) != current_set_->end();
}

void HotKeySet::UpdateHotSet() {
  // bind_core_on_numa(num_workers_);
  LOG("begin update hot set");

  std::unordered_map<uint64_t, int> count;
  uint64_t update_cnt = 0;
  while (!stop_flag_.load(std::memory_order_relaxed)) {
    if (count.size() > HOT_NUM * 4 || update_cnt > HOT_NUM * 16) {
      break;
    }
    std::list<std::vector<uint64_t>> list;
    for (int i = 0; i < num_workers_; i++) {
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

  std::unordered_set<uint64_t> *old_set = current_set_;
  std::unordered_set<uint64_t> *new_set = nullptr;
  if (!topK.empty()) {
    if (max_cnt > 3 * topK.top().cnt) {
      new_set = new std::unordered_set<uint64_t>();
      new_set->reserve(topK.size());
      while (!topK.empty()) {
        new_set->insert(topK.top().key);
        topK.pop();
      }
      LOG("new set size %lu", new_set->size());
    }
  }

  current_set_ = new_set;
  // no rcu here, simply sleep for a while
  usleep(10);
  for (int i = 0; i < num_workers_; i++) {
    // need_record_ is false, other threads cannot operate on records
    update_record_[i].records.clear();
    update_record_[i].records_list.clear();
    update_record_[i].hit_cnt = update_record_[i].total_cnt = 0;
  }
  if (old_set) {
    delete old_set;
  }

  update_schedule_flag_.clear(std::memory_order_relaxed);
  need_count_hit_ = true;
}
