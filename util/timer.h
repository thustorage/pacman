#pragma once

#include <chrono>
#include <sys/time.h>

static inline uint64_t NowMicros() {
  static constexpr uint64_t kUsecondsPerSecond = 1000000;
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}

#define TIMER_START(x)                                                         \
  const auto timer_##x = std::chrono::steady_clock::now()

#define TIMER_STOP(x)                                                          \
  x += std::chrono::duration_cast<std::chrono::nanoseconds>(                   \
           std::chrono::steady_clock::now() - timer_##x)                       \
           .count()

template <typename T>
struct Timer {
  Timer(T &res) : start_time_(std::chrono::steady_clock::now()), res_(res) {}

  ~Timer() {
    res_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - start_time_)
                .count();
  }

  std::chrono::steady_clock::time_point start_time_;
  T &res_;
};

#ifdef LOGGING
#define TIMER_START_LOGGING(x) TIMER_START(x)
#define TIMER_STOP_LOGGING(x) TIMER_STOP(x)
#define COUNTER_ADD_LOGGING(x, y) x += (y)
#else
#define TIMER_START_LOGGING(x)
#define TIMER_STOP_LOGGING(x)
#define COUNTER_ADD_LOGGING(x, y)
#endif
