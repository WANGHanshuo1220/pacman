#pragma once

#include <cstdint>
#include <atomic>
#include <libpmem.h>

#include "config.h"
#include "db_common.h"
#include "util/util.h"

#define TIMEDIFF(s, e) (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec) //us

// static constexpr int NUM_HEADERS = 1;
#ifdef INTERLEAVED
static constexpr int HEADER_ALIGN_SIZE = 4;
#else
static constexpr int HEADER_ALIGN_SIZE = 256;
#endif
// weird slow when 4 * 64
// rotating counter with multi logs reduce performance

/**
 * segment header
 * tail pointer (offset): 4 bytes
 * status: free, in-used, close
 */
enum SegmentStatus { StatusAvailable, StatusUsing, StatusClosed };
class BaseSegment {
 public:

  struct alignas(HEADER_ALIGN_SIZE) Header {
#ifdef INTERLEAVED
    uint8_t status;
#else
    uint32_t offset; // only valid when status is closed
    uint32_t status;
    uint32_t objects_tail_offset;
#endif
    bool has_shortcut;

    void Flush() {
#ifdef LOG_PERSISTENT
      clflushopt_fence(this, sizeof(Header));
#endif
    }
  };

  static constexpr uint32_t HEADERS_SIZE = sizeof(Header);
  static constexpr uint32_t SEGMENT_DATA_SIZE = (SEGMENT_SIZE - HEADERS_SIZE);
  static constexpr uint32_t BYTES_PER_BIT = 32;
  static constexpr uint32_t BITMAP_SIZE =
      (SEGMENT_DATA_SIZE / BYTES_PER_BIT + 7) / 8;

  int cur_cnt_ = 0;
#ifdef GC_EVAL
  long make_new_kv_time = 0;
  long b1 = 0;
  long b2 = 0;
  long b3 = 0;
  long b4 = 0;
#endif


  BaseSegment(char *start_addr, size_t size)
      : segment_start_(start_addr),
        data_start_(start_addr + HEADERS_SIZE),
        end_(start_addr + size) {}

  virtual ~BaseSegment() {}

  bool HasSpaceFor(uint32_t sz) {
    char *tmp_end = tail_ + sz;
#ifdef GC_SHORTCUT
    if (has_shortcut_) {
      tmp_end += sizeof(Shortcut) * (cur_cnt_ + 1);
    }
#endif
    return tmp_end <= end_;
  }

  char *AllocOne(size_t size) {
    char *ret = tail_;
    if (ret + size < end_) {
      tail_ += size;
      ++cur_cnt_;
      return ret;
    } else {
      return nullptr;
    }
  }

  char *AllocSpace(size_t size) {
    char *ret = tail_;
    if (ret + size < end_) {
      tail_ += size;
      return ret;
    } else {
      return nullptr;
    }
  }

  uint32_t get_offset() { return tail_ - data_start_; }

  char *get_segment_start() { return segment_start_; }

  char *get_data_start() { return data_start_; }

  char *get_tail() { return tail_; }

  char *get_end() { return end_; }

  bool HasShortcut() { return has_shortcut_; }
  void set_has_shortcut(bool has_shortcut) { has_shortcut_ = has_shortcut; }

 protected:
  union {
    char *const segment_start_; // const
    Header *header_;
  };
  char *const data_start_;
  char *const end_; // const
// #ifdef INTERLEAVED
//   std::atomic<char *> tail_;
// #else
  char *tail_;
// #endif
  
  bool has_shortcut_ = false;

  DISALLOW_COPY_AND_ASSIGN(BaseSegment);
};

class LogSegment : public BaseSegment {
 public:
  LogSegment(char *start_addr, uint64_t size, bool init = true)
      : BaseSegment(start_addr, size), garbage_bytes_(0) {
    assert(((uint64_t)header_ & (HEADER_ALIGN_SIZE - 1)) == 0);
    if (init) {
      Init();
    }
  }

#ifdef INTERLEAVED
  bool is_segment_closed() { return header_->status == StatusClosed; }
  uint16_t num_kvs = 0;
  uint32_t roll_back_c = 0;
  // vector for pairs <IsGarbage, kv_size>
  std::vector<std::pair<bool, uint16_t>> roll_back_map
    = std::vector<std::pair<bool, uint16_t>>(SEGMENT_SIZE/32);
#endif

  void Init() {
    tail_ = data_start_;
    // header_->offset = 0;
    // header_->objects_tail_offset = 0;
    header_->status = StatusAvailable;
    header_->Flush();
    InitBitmap();
#ifdef LOG_BATCHING
    flush_tail_ = data_start_;
#endif
  }

  void InitBitmap() {
#ifdef REDUCE_PM_ACCESS
    if (volatile_tombstone_) {
      free(volatile_tombstone_);
    }
    volatile_tombstone_ = (uint8_t *)malloc(BITMAP_SIZE);
    memset(volatile_tombstone_, 0, BITMAP_SIZE);
#endif
  }

#ifdef INTERLEAVED
  void update_Bitmap()
  {
#ifdef REDUCE_PM_ACCESS
  char *t = tail_;
  int idx = (t - data_start_) / BYTES_PER_BIT;
  int byte = idx / 8;
  int bit = idx % 8;
  uint8_t a = 0b11111111 >> (8 - bit);
  uint8_t old_val = volatile_tombstone_[byte];
  // printf("tail old val = %d\n", old_val);
  uint8_t new_val = old_val & a;
  // printf("tail new val = %d\n", new_val);
  while (true) {
    if (__atomic_compare_exchange_n(&volatile_tombstone_[byte], &old_val,
                                    new_val, true, __ATOMIC_ACQ_REL,
                                    __ATOMIC_ACQUIRE)) {
      break;
    }
  }
  new_val = 0b00000000;
  for(int i = byte + 1; i < BITMAP_SIZE; i++)
  {
    old_val = volatile_tombstone_[i];
    // printf("old val = %d\n", volatile_tombstone_[i]);
    while (true) {
      if (__atomic_compare_exchange_n(&volatile_tombstone_[i], &old_val,
                                      new_val, true, __ATOMIC_ACQ_REL,
                                      __ATOMIC_ACQUIRE)) {
        break;
      }
    }
    // printf("new val = %d\n", volatile_tombstone_[i]);
  }
  // printf("-----------------------\n");
#endif
  }

  void roll_back_tail(uint32_t sz) { 
    std::lock_guard<SpinLock> guard(tail_lock);
    tail_ -= sz; 
  }

  void set_is_free_seg(bool a) { is_free_seg = a; }
  // void set_has_been_RB(bool a) { has_been_RB = a; }

#endif

  void InitShortcutBuffer() {
#ifdef GC_SHORTCUT
    if (shortcut_buffer_) {
      delete shortcut_buffer_;
    }
    if (has_shortcut_) {
      shortcut_buffer_ = new std::vector<Shortcut>();
      shortcut_buffer_->reserve(SEGMENT_DATA_SIZE / 8);
    }
#endif
  }

  virtual ~LogSegment() {
#ifdef REDUCE_PM_ACCESS
    if (volatile_tombstone_) {
      free(volatile_tombstone_);
    }
#endif
#ifdef GC_SHORTCUT
    if (shortcut_buffer_) {
      delete shortcut_buffer_;
      shortcut_buffer_ = nullptr;
    }
#endif
  }

  void StartUsing(bool is_hot, bool has_shortcut = false) {
    header_->status = StatusUsing;
    header_->has_shortcut = has_shortcut;
    header_->Flush();
    is_hot_ = is_hot;
#ifdef GC_SHORTCUT
    has_shortcut_ = has_shortcut;
    InitShortcutBuffer();
#endif
  }

  void Close() {
    // if (HasSpaceFor(sizeof(KVItem))) {
    //   KVItem *end = new (tail_) KVItem();
    //   end->Flush();
    //   tail_ += sizeof(KVItem);
    // }
#ifdef GC_SHORTCUT
    if (shortcut_buffer_) {
      assert(tail_ + shortcut_buffer_->size() * sizeof(Shortcut) <= end_);
      pmem_memcpy_persist(tail_, shortcut_buffer_->data(),
                          shortcut_buffer_->size() * sizeof(Shortcut));
      assert(shortcut_buffer_->size() == cur_cnt_);
      delete shortcut_buffer_;
      shortcut_buffer_ = nullptr;
    }
#endif
    close_time_ = NowMicros();
    // header_->offset = get_offset();
    header_->status = StatusClosed;
    // header_->objects_tail_offset = get_offset();
    header_->has_shortcut = has_shortcut_;
    header_->Flush();
  }

  void Clear() {
    tail_ = data_start_;
    garbage_bytes_ = 0;
    is_hot_ = false;
    header_->status = StatusAvailable;
    // header_->objects_tail_offset = 0;
    header_->has_shortcut = false;
    header_->Flush();
#ifdef INTERLEAVED
    num_kvs = 0;
    roll_back_map.clear();
#endif
#ifdef GC_EVAL
    make_new_kv_time = 0;
#endif
#ifdef LOG_BATCHING
    not_flushed_cnt_ = 0;
    flush_tail_ = data_start_;
#endif
    has_shortcut_ = false;
    cur_cnt_ = 0;
#ifdef REDUCE_PM_ACCESS
    if (volatile_tombstone_) {
      memset(volatile_tombstone_, 0, BITMAP_SIZE);
    }
#endif
  }

  // append kv to log
  ValueType Append(const Slice &key, const Slice &value, uint32_t epoch) {
    uint32_t sz = sizeof(KVItem) + key.size() + value.size();
    if (!HasSpaceFor(sz)) {
      // printf("segment has no space\n");
      return INVALID_VALUE;
    }
#ifdef GC_EVAL
    struct timeval make_new_kv_start, p1, p2, p3;
    struct timeval make_new_kv_end;
    gettimeofday(&make_new_kv_start, NULL);
#endif
#ifdef INTERLEAVED
    uint16_t cur_num = num_kvs;
// #ifdef GC_EVAL
//     gettimeofday(&p1, NULL);
// #endif
    std::lock_guard<SpinLock> guard(tail_lock);
    KVItem *kv = new (tail_) KVItem(key, value, epoch, cur_num);
// #ifdef GC_EVAL
//     gettimeofday(&p2, NULL);
// #endif
    // roll_back_map.push_back(std::pair<bool, uint32_t>(false, sz));
    // roll_back_map[cur_num] = std::pair<bool, uint32_t>(false, sz);
    roll_back_map[cur_num].first = false;
    roll_back_map[cur_num].second = sz;
// #ifdef GC_EVAL
//     gettimeofday(&p3, NULL);
// #endif
    num_kvs ++;
#else
    KVItem *kv = new (tail_) KVItem(key, value, epoch);
#endif
#ifdef GC_EVAL
    gettimeofday(&make_new_kv_end, NULL);
    make_new_kv_time += (make_new_kv_end.tv_sec - make_new_kv_start.tv_sec) * 1000000 + (make_new_kv_end.tv_usec - make_new_kv_start.tv_usec);
// #ifdef INTERLEAVED
//     b1 += TIMEDIFF(make_new_kv_start, p1);
//     b2 += TIMEDIFF(p1, p2);
//     b3 += TIMEDIFF(p2, p3);
//     b4 += TIMEDIFF(p3, make_new_kv_end);
// #endif
#endif
    // printf("Append kv:");
    // printf("  seg_start = %p\n", get_segment_start());
    // printf("  old_tail  = %p\n", get_tail());
    kv->Flush();
    tail_ += sz;
    ++cur_cnt_;
#ifdef INTERLEAVED
    return TaggedPointer((char *)kv, sz, cur_num);
#else
    return TaggedPointer((char *)kv, sz, -1);
#endif
  }

#ifdef LOG_BATCHING
  int FlushRemain() {
    clwb_fence(flush_tail_, tail_ - flush_tail_);
    flush_tail_ = tail_;
    int persist_cnt = not_flushed_cnt_;
    not_flushed_cnt_ = 0;
    return persist_cnt;
  }

  ValueType AppendBatchFlush(const Slice &key, const Slice &value,
                             uint32_t epoch, int *persist_cnt) {
    uint32_t sz = sizeof(KVItem) + key.size() + value.size();
    if (!HasSpaceFor(sz)) {
      return INVALID_VALUE;
    }
#ifdef INTERLEAVED
    KVItem *kv = new (tail_) KVItem(key, value, epoch, num_kvs);
    // roll_back_map.push_back(std::pair<bool, uint32_t>(false, sz));
    roll_back_map[num_kvs] = std::pair<bool, uint32_t>(false, sz);
    num_kvs ++;
#else
    KVItem *kv = new (tail_) KVItem(key, value, epoch);
#endif
    ++not_flushed_cnt_;
    tail_ += sz;
    ++cur_cnt_;
    char *align_addr = (char *)((uint64_t)tail_ & ~(LOG_BATCHING_SIZE - 1));
    if (align_addr - flush_tail_ >= LOG_BATCHING_SIZE) {
#ifdef INTERLEAVED
      clwb_fence(flush_tail_, tail_ - flush_tail_);
      flush_tail_ = tail_;
      *persist_cnt = not_flushed_cnt_;
      not_flushed_cnt_ = 0;
#else
      clwb_fence(flush_tail_, align_addr - flush_tail_);
      flush_tail_ = align_addr;
      if (tail_ == align_addr) {
        *persist_cnt = not_flushed_cnt_;
        not_flushed_cnt_ = 0;
      } else {
        *persist_cnt = not_flushed_cnt_ - 1;
        not_flushed_cnt_ = 1;
      }
#endif
    } else {
      *persist_cnt = 0;
    }
    return TaggedPointer((char *)kv, sz);
  }
#endif

#ifdef GC_SHORTCUT
  void AddShortcut(Shortcut sc) {
    if (shortcut_buffer_) {
      shortcut_buffer_->push_back(sc);
    }
  }
#endif

  double GetGarbageProportion() {
    return (double)(garbage_bytes_.load(std::memory_order_relaxed)) /
           get_offset();
  }

  uint64_t get_close_time() { return close_time_; }

  bool IsGarbage(char *p) {
#ifdef REDUCE_PM_ACCESS
    assert(p >= data_start_ && p < tail_);
    int idx = (p - data_start_) / BYTES_PER_BIT;
    int byte = idx / 8;
    int bit = idx % 8;
    return (volatile_tombstone_[byte] >> bit) & 1;
#endif
    assert(0);
    return false;
  }

  void MarkGarbage(char *p, uint32_t sz) {
#ifdef WRITE_TOMBSTONE
    char *t = tail_;
    if((uint64_t)p < (uint64_t)data_start_ || (uint64_t)p >= (uint64_t)t)
    {
      int idx = (p - data_start_) / BYTES_PER_BIT;
      int byte = idx / 8;
      int bit = idx % 8;
      printf("is_free_seg= %d\n", is_free_seg);
      printf("roll_bac   = %d\n", roll_back_c);
      printf("num_kvs    = %d\n", num_kvs);
      printf("p          = %p\n", p);
      printf("data_start = %p\n", data_start_);
      printf("t          = %p\n", t);
      printf("tail_      = %p\n", tail_);
      printf("idx        = %d\n", idx); 
      printf("byte       = %d\n", byte);
      printf("v_tomb     = %d\n", volatile_tombstone_[byte]);
      printf("bit        = %d\n", bit);
    }
    assert(p >= data_start_ && p < tail_);
#ifdef REDUCE_PM_ACCESS
    int idx = (p - data_start_) / BYTES_PER_BIT;
    int byte = idx / 8;
    int bit = idx % 8;
    uint8_t old_val = volatile_tombstone_[byte];
    while (true) {
      assert(((old_val >> bit) & 1) == 0);
      uint8_t new_val = old_val | (1 << bit);
      if (__atomic_compare_exchange_n(&volatile_tombstone_[byte], &old_val,
                                      new_val, true, __ATOMIC_ACQ_REL,
                                      __ATOMIC_ACQUIRE)) {
        break;
      }
    }
#else
    KVItem *kv = reinterpret_cast<KVItem *>(p);
    // assert(kv->is_garbage == false); // may copy a object in gc
    // assert(kv->magic == 0xDEADBEAF);
    kv->is_garbage = true;
    // pmem_clwb((const void *)&kv->is_garbage);
#endif
#endif
    garbage_bytes_ += sz;
  }

  // void set_is_hot(bool hot) { is_hot_ = hot; }
  bool IsHot() { return is_hot_; }

 private:
  uint64_t close_time_;
#ifdef REDUCE_PM_ACCESS
  uint8_t *volatile_tombstone_ = nullptr;
#endif

#ifdef INTERLEAVED
  SpinLock tail_lock;
  bool is_free_seg = false;
  // bool has_been_RB = false;
#endif

  std::atomic<int> garbage_bytes_;
  bool is_hot_ = false;

#ifdef GC_SHORTCUT
  std::vector<Shortcut> *shortcut_buffer_ = nullptr;
#endif
#ifdef LOG_BATCHING
  int not_flushed_cnt_ = 0;
  char *flush_tail_ = nullptr;
#endif

  friend class LogCleaner;
  DISALLOW_COPY_AND_ASSIGN(LogSegment);
};

class VirtualSegment : public BaseSegment {
 public:
  VirtualSegment(uint64_t size) : BaseSegment((char *)malloc(size), size) {
    // volatile segment used for cleaner nt-copy
    tail_ = data_start_;
  }

  ~VirtualSegment() { 
    printf("~VirtualSegemnt begin\n");
    printf("data_begin = %p (%d)\n", data_start_, *(int *)data_start_);
    printf("tail_ = %p\n", tail_);
    printf("end_ = %p\n", end_);
    free(segment_start_); 
    printf("~VirtualSegemnt end\n");
  }

  void Clear() {
    tail_ = data_start_;
    cur_cnt_ = 0;
  }
};

struct SegmentInfo {
  LogSegment *segment;
  double compaction_score = 0.0;

  bool operator<(const SegmentInfo &other) const {
    return compaction_score > other.compaction_score;
  }
};
