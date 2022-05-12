#if 0
#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

// variable-sized Key

#if KEY_SIZE == 8
using raw_key_t = uint64_t;
using KeyType = uint64_t;
#else
using raw_key_t = VarKey<KEY_SIZE>;
using KeyType = VarKeyPtr<KEY_SIZE>;
#endif


static inline KeyType to_Key_t(const Slice &key) {
#if KEY_SIZE == 8
  return *(uint64_t *)key.data();
#else
  return KeyType((raw_key_t *)key.data());
#endif
}

static inline uint64_t get_u64_value(const KeyType &key) {
#if KEY_SIZE == 8
  return key;
#else
  return key.GetKey();
#endif
}

static inline const raw_key_t *get_raw_key_addr(const KeyType &key) {
#if KEY_SIZE == 8
  return &key;
#else
  return key.key_ptr;
#endif
}


template <size_t N>
struct VarKey {
  static_assert(
      N % sizeof(uint64_t) == 0,
      "The size should be multiple of sizeof(uint64_t)"); // for simplicity
  static constexpr size_t sz = N / sizeof(uint64_t);

  // uint64_t data[sz];
  std::array<uint64_t, sz> data;

  VarKey(const uint64_t &k) {
    for (size_t i = 0; i < sz; i++) {
      data[i] = k;
    }
  }

  VarKey &operator=(const uint64_t &k) {
    for (size_t i = 0; i < sz; i++) {
      data[i] = k;
    }
    return *this;
  }

  uint64_t GetKey() const {
    return data[0];
  }

  // operator uint64_t() const {
  //   return GetKey();
  // }

  bool operator==(const VarKey &other) const {
    return GetKey() == other.GetKey();
  }
  
  bool operator!=(const VarKey &other) const {
    return GetKey() != other.GetKey();
  }

  bool operator<(const VarKey &other) const {
    return GetKey() < other.GetKey();
  }

  bool operator>(const VarKey &other) const {
    return GetKey() > other.GetKey();
  }

  bool operator<=(const VarKey &other) const {
    return GetKey() <= other.GetKey();
  }

  bool operator>=(const VarKey &other) const {
    return GetKey() >= other.GetKey();
  }
};

template <size_t N>
struct VarKeyPtr {
  union {
    const struct VarKey<N> *key_ptr;
    uint64_t data;
  };

  VarKeyPtr() : key_ptr(nullptr) {}

  VarKeyPtr(const struct VarKey<N> *p) : key_ptr(p) {}

  uint64_t GetKey() const {
    return key_ptr->GetKey();
  }

  bool operator==(const VarKeyPtr &other) const {
    return *key_ptr == *(other.key_ptr);
  }

  bool operator!=(const VarKeyPtr &other) const {
    return *key_ptr != *(other.key_ptr);
  }

  bool operator<(const VarKeyPtr &other) const {
    return *key_ptr < *(other.key_ptr);
  }

  bool operator>(const VarKeyPtr &other) const {
    return *key_ptr > *(other.key_ptr);
  }

  bool operator<=(const VarKeyPtr &other) const {
    return *key_ptr <= *(other.key_ptr);
  }

  bool operator>=(const VarKeyPtr &other) const {
    return *key_ptr >= *(other.key_ptr);
  }
};

#endif
