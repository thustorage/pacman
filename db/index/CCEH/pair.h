#pragma once

#include <cstdlib>

#include "db_common.h"

typedef uint64_t Key_t;
typedef uint64_t Value_t;

const Key_t SENTINEL = -2; // 11111...110
const Key_t INVALID = -1;  // 11111...111

const Value_t NONE = 0x0;

struct Pair {
  Key_t key;
  Value_t value;

  Pair(void) : key{INVALID} {}

  Pair(Key_t _key, Value_t _value) : key{_key}, value{_value} {}

  Pair &operator=(const Pair &other) {
    key = other.key;
    value = other.value;
    return *this;
  }

  void *operator new(size_t size) = delete;

  void *operator new[](size_t size) = delete;
};
