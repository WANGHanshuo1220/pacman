#pragma once

#include <cstdint>
#include <tuple>
#include <iostream>

#include "config.h"
#include "slice.h"
#include "util/debug_helper.h"
#include "util/persist.h"
#include "util/index_arena.h"

// key, value in Index
using KeyType = uint64_t;
using ValueType = uint64_t;
static constexpr ValueType INVALID_VALUE = 0;

static constexpr uint64_t num_class     = 3;
static constexpr uint64_t kv_align      = 4;
static constexpr uint64_t hash_sz       = 20;
static constexpr uint64_t SEGMENT_SIZE[num_class] = 
    {4ul << 20, 1ul << 18, 1ul << 16};
    // {4ul << 20, 1ul << 16, 1ul << 14};

// shortcut
class __attribute__((__packed__)) Shortcut {
 public:
  Shortcut() : addr_(0), pos_(0) {}
  Shortcut(char *node_addr, int pos) {
    assert(((uint64_t)node_addr & 0xf) == 0);
#ifdef IDX_PERSISTENT
    addr_ = g_index_allocator->ToPoolOffset(node_addr);
#else
    addr_ = (uint64_t)node_addr >> 4;
#endif
    pos_ = pos;
  }

  char *GetNodeAddr() const {
#ifdef IDX_PERSISTENT
    return addr_ ? (char *)g_index_allocator->ToDirectPointer(addr_) : nullptr;
#else
    return (char *)((uint64_t)addr_ << 4);
#endif
  }

  int GetPos() const { return pos_; }

  bool None() { return addr_ == 0; }

 private:
  uint64_t addr_ : 43;
  uint8_t pos_ : 5;
};
static_assert(sizeof(Shortcut) == 6);


// KVItem: log entry
struct alignas(kv_align) KVItem {
  uint16_t key_size;
  uint16_t val_size;
  // uint32_t checksum = 0;
  // uint64_t epoch;
  uint32_t epoch;
  uint32_t num; // max kvs in a segment = 2^16
  // uint64_t magic = 0xDEADBEAF;
  uint8_t kv_pair[0];

  KVItem() {
    memset(this, 0, sizeof(KVItem));
  }

  KVItem(const Slice &_key, const Slice &_val, uint32_t _epoch, uint32_t _num)
      : key_size(_key.size()), val_size(_val.size()), epoch(_epoch), num(_num) {
    assert(val_size >= 8);
    memcpy(kv_pair, _key.data(), key_size);
    memcpy(kv_pair + key_size, _val.data(), val_size);
    // CalcChecksum();
  }

  Slice GetKey() {
    return Slice((char *)kv_pair, key_size);
  }

  Slice GetValue() {
    return Slice((char *)kv_pair + key_size, val_size);
  }

  void GetValue(std::string &value) {
    value.assign((char *)kv_pair + key_size, val_size);
  }

  // void CalcChecksum() {
  //   // checksum = 0;
  //   // uint64_t *p = (uint64_t *)this;
  //   // uint64_t x_sum = 0;
  //   // size_t sz = sizeof(KVItem) + key_size + val_size;
  //   // for (size_t i = 0; i < sz / sizeof(uint64_t); i++) {
  //   //   x_sum ^= p[i];
  //   // }
  //   // checksum = x_sum ^ (x_sum >> 32);
  // }

  // bool VerifyChecksum() {
  //   // uint64_t *p = (uint64_t *)this;
  //   // uint64_t x_sum = 0;
  //   // size_t sz = sizeof(KVItem) + key_size + val_size;
  //   // for (size_t i = 0; i < sz / sizeof(uint64_t); i++) {
  //   //   x_sum ^= p[i];
  //   // }
  //   // uint32_t res = x_sum ^ (x_sum >> 32);
  //   // return (res == 0);
  //   return true;
  // }

  void Flush() {
#ifdef LOG_PERSISTENT
    clwb_fence((char *)this, sizeof(KVItem) + key_size + val_size);
#endif
  }
};


// TaggedPointer
struct TaggedPointer {
  union {
    uint64_t data = 0;
    struct {
      uint64_t addr : 48;
      uint64_t size_or_num : 16;
    };
  };

  TaggedPointer(char *ptr, uint64_t sz, uint64_t num_, int class_t) {
    addr = (uint64_t)ptr;
    if(class_t != 0)
    {
      size_or_num = num_ < 0xFFFF ? num_ : 0xFFFF;
    }
    else
    {
      size_or_num = sz <= 0xFFFF ? sz : 0;
    }
  }

  TaggedPointer(ValueType val) : data(val) {}

  operator ValueType() {
    return (ValueType)data;
  }
  KVItem *GetKVItem() {
    return (KVItem *)(uint64_t)addr;
  }
  char *GetAddr() {
    return (char *)(uint64_t)addr;
  }
};


struct LogEntryHelper {
  ValueType new_val = INVALID_VALUE;
  ValueType old_val = INVALID_VALUE;  // in and out for gc put, out for db put
  Shortcut shortcut;
  char *index_entry = nullptr;
  bool fast_path = false;

  LogEntryHelper(ValueType _new_val) : new_val(_new_val) {}
};

struct ValidItem {
  Slice key;
  ValueType old_val;
  ValueType new_val;
  uint32_t size;
  Shortcut shortcut;

  ValidItem(const Slice &key, ValueType old_val, ValueType new_val,
            uint32_t size, Shortcut shortcut)
      : key(key),
        old_val(old_val),
        new_val(new_val),
        size(size),
        shortcut(shortcut) {}

  // bool operator<(const ValidItem &other) const {
  //   return shortcut < other.shortcut;
  // }
};


struct record_info
{
  union
  {
    uint16_t data = 0;
    struct
    {
      uint16_t kv_sz      : 15;
      uint16_t is_garbage : 1;
    };
  };
};

