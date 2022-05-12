#pragma once

#include <cstdint>
#include <cstddef>
#include "util/debug_helper.h"

class Random {
 private:
  uint32_t seed_;

 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;  // 2^31-1
    static const uint64_t A = 16807;        // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) { return Uniform(1 << Uniform(max_log + 1)); }
};

enum OP_Type {
  OP_Read, OP_Update, OP_Insert, OP_Scan
};

enum YCSB_Type {
  YCSB_A,
  YCSB_B,
  YCSB_C,
  YCSB_E,
  YCSB_W20,
  YCSB_W40,
  YCSB_W60,
  YCSB_W80,
  YCSB_W100,
  YCSB_Type_END,
  YCSB_W0 = YCSB_C
};

static const char *ycsb_name[] = {"YCSB_A",   "YCSB_B",   "YCSB_C (YCSB_W0)",
                                  "YCSB_E",   "YCSB_W20", "YCSB_W40",
                                  "YCSB_W60", "YCSB_W80", "YCSB_W100"};

static constexpr int YCSB_Put_Ratio[YCSB_Type_END] = {50, 5,  0,  5,  20,
                                                      40, 60, 80, 100};

inline OP_Type get_op_type(Random *rand, YCSB_Type type) {
  if (type == YCSB_W100) {
    return OP_Update;
  } else if (type == YCSB_C) {
    return OP_Read;
  }
  OP_Type t;
  uint32_t k = rand->Next() % 100;
  switch (type) {
    case YCSB_A:
    case YCSB_B:
    case YCSB_W20:
    case YCSB_W40:
    case YCSB_W60:
    case YCSB_W80: {
      if (k < YCSB_Put_Ratio[type]) {
        t = OP_Update;
      } else {
        t = OP_Read;
      }
      break;
    }
    case YCSB_E: {
      if (k < YCSB_Put_Ratio[type]) {
        t = OP_Update;
      } else {
        t = OP_Scan;
      }
      break;
    }
    default:
      ERROR_EXIT("not supported");
      break;
  }

  return t;
}

/**
 * Facebook ETC
 * 40% 1~13 bytes
 * 55% 14~300 bytes
 * 5% > 300 bytes (up to 4000)
 * return kind
 */
enum class ETC_Kind { small, medium, large };

inline ETC_Kind ETC_get_kind(Random &rand) {
  int k = rand.Uniform(100);
  if (k < 40) {
    return ETC_Kind::small;
  } else if (k < 95) {
    return ETC_Kind::medium;
  } else {
    return ETC_Kind::large;
  }
}

size_t ETC_get_value_size(Random &rand, ETC_Kind kind) {
  if (kind == ETC_Kind::small) {
    return 8;
  } else if (kind == ETC_Kind::medium) {
    return 8 * (2 + rand.Uniform(36));
  } else {
    return 8 * (38 + rand.Uniform(463));
  }
}

static constexpr size_t ETC_LARGE_VALUE_BOUNDARY = 300;
static constexpr double ETC_AVG_VALUE_SIZE =
    0.4 * 8 + 0.55 * (16 + 296) / 2 + 0.05 * (304 + 4000) / 2;
