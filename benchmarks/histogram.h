#pragma once

#include <vector>
#include <map>
#include <cmath>
#include <chrono>
#include <cstdint>
#include <cassert>

namespace HistogramInternal {

class HistogramBucketMapper {
 public:
  HistogramBucketMapper();

  // converts a value to the bucket index.
  size_t IndexForValue(uint64_t value) const;

  // number of buckets required.
  size_t BucketCount() const { return bucketValues_.size(); }

  uint64_t LastValue() const { return maxBucketValue_; }

  uint64_t FirstValue() const { return minBucketValue_; }

  uint64_t BucketLimit(const size_t bucketNumber) const {
    assert(bucketNumber < BucketCount());
    return bucketValues_[bucketNumber];
  }

 private:
  std::vector<uint64_t> bucketValues_;
  uint64_t maxBucketValue_;
  uint64_t minBucketValue_;
  std::map<uint64_t, uint64_t> valueIndexMap_;
};

} // namespace HistogramInternal

struct HistogramData {
  double median;
  double percentile95;
  double percentile99;
  double average;
  double standard_deviation;
  // zero-initialize new members since old Statistics::histogramData()
  // implementations won't write them.
  double max = 0.0;
  uint64_t count = 0;
  uint64_t sum = 0;
  double min = 0.0;
};

// not thread-safe
struct Histogram {
  Histogram();
  ~Histogram() {}

  Histogram(const Histogram&) = delete;
  Histogram& operator=(const Histogram&) = delete;

  void Clear();
  bool Empty() const;
  void Add(uint64_t value);
  void Merge(const Histogram& other);

  inline uint64_t min() const { return min_; }
  inline uint64_t max() const { return max_; }
  inline uint64_t num() const { return num_; }
  inline uint64_t sum() const { return sum_; }
  inline uint64_t sum_squares() const { return sum_squares_; }
  inline uint64_t bucket_at(size_t b) const { return buckets_[b];}

  double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;
  void Data(HistogramData *const data) const;

  // To be able to use Histogram as thread local variable, it
  // cannot have dynamic allocated member. That's why we're
  // using manually values from BucketMapper
  std::uint64_t min_;
  std::uint64_t max_;
  std::uint64_t num_;
  std::uint64_t sum_;
  std::uint64_t sum_squares_;
  std::uint64_t buckets_[109]; // 109==BucketMapper::BucketCount()
  const uint64_t num_buckets_;
};


class StopWatch {
 public:
  StopWatch(Histogram *hist) : hist_(hist) {
    if (hist_) {
      start_time_ = std::chrono::steady_clock::now();
    }
  }

  ~StopWatch() {
    if (hist_) {
      uint64_t duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - start_time_)
                              .count();
      hist_->Add(duration);
    }
  }

  StopWatch(const StopWatch&) = delete;
  StopWatch& operator=(const StopWatch&) = delete;

 private:
  Histogram *hist_;
  std::chrono::steady_clock::time_point start_time_;
};
