#include "histogram.h"

namespace HistogramInternal {

HistogramBucketMapper::HistogramBucketMapper() {
  // If you change this, you also need to change
  // size of array buckets_ in HistogramImpl
  bucketValues_ = {1, 2};
  valueIndexMap_ = {{1, 0}, {2, 1}};
  double bucket_val = static_cast<double>(bucketValues_.back());
  while ((bucket_val = 1.5 * bucket_val) <= static_cast<double>(UINT64_MAX)) {
    bucketValues_.push_back(static_cast<uint64_t>(bucket_val));
    // Extracts two most significant digits to make histogram buckets more
    // human-readable. E.g., 172 becomes 170.
    uint64_t pow_of_ten = 1;
    while (bucketValues_.back() / 10 > 10) {
      bucketValues_.back() /= 10;
      pow_of_ten *= 10;
    }
    bucketValues_.back() *= pow_of_ten;
    valueIndexMap_[bucketValues_.back()] = bucketValues_.size() - 1;
  }
  maxBucketValue_ = bucketValues_.back();
  minBucketValue_ = bucketValues_.front();
}

size_t HistogramBucketMapper::IndexForValue(uint64_t value) const {
  if (value >= maxBucketValue_) {
    return bucketValues_.size() - 1;
  } else if (value >= minBucketValue_) {
    std::map<uint64_t, uint64_t>::const_iterator lowerBound =
        valueIndexMap_.lower_bound(value);
    if (lowerBound != valueIndexMap_.end()) {
      return static_cast<size_t>(lowerBound->second);
    } else {
      return 0;
    }
  } else {
    return 0;
  }
}

} // namespace HistogramInternal

namespace {
  const HistogramInternal::HistogramBucketMapper bucketMapper;
}

Histogram::Histogram() : num_buckets_(bucketMapper.BucketCount()) {
  assert(num_buckets_ == sizeof(buckets_) / sizeof(*buckets_));
  Clear();
}

void Histogram::Clear() {
  min_ = bucketMapper.LastValue();
  max_ = 0;
  num_ = 0;
  sum_ = 0;
  sum_squares_ = 0;
  for (unsigned int b = 0; b < num_buckets_; b++) {
    buckets_[b] = 0;
  }
}

bool Histogram::Empty() const { return num() == 0; }

void Histogram::Add(uint64_t value) {
  // This function is designed to be lock free, as it's in the critical path
  // of any operation. Each individual value is atomic and the order of
  // updates by concurrent threads is tolerable.
  const size_t index = bucketMapper.IndexForValue(value);
  assert(index < num_buckets_);
  buckets_[index] += 1;

  min_ = std::min(min_, value);
  max_ = std::max(max_, value);

  num_ += 1;
  sum_ += value;
  sum_squares_ += value * value;
}

void Histogram::Merge(const Histogram &other) {
  min_ = std::min(min_, other.min_);
  max_ = std::max(max_, other.max_);
  num_ += other.num_;
  sum_ += other.sum_;
  sum_squares_ += other.sum_squares_;
  for (unsigned int b = 0; b < num_buckets_; b++) {
    buckets_[b] += other.buckets_[b];
  }
}

double Histogram::Median() const { return Percentile(50.0); }

double Histogram::Percentile(double p) const {
  double threshold = num() * (p / 100.0);
  uint64_t cumulative_sum = 0;
  for (unsigned int b = 0; b < num_buckets_; b++) {
    uint64_t bucket_value = bucket_at(b);
    cumulative_sum += bucket_value;
    if (cumulative_sum >= threshold) {
      // Scale linearly within this bucket
      uint64_t left_point = (b == 0) ? 0 : bucketMapper.BucketLimit(b - 1);
      uint64_t right_point = bucketMapper.BucketLimit(b);
      uint64_t left_sum = cumulative_sum - bucket_value;
      uint64_t right_sum = cumulative_sum;
      double pos = 0;
      uint64_t right_left_diff = right_sum - left_sum;
      if (right_left_diff != 0) {
        pos = (threshold - left_sum) / right_left_diff;
      }
      double r = left_point + (right_point - left_point) * pos;
      uint64_t cur_min = min();
      uint64_t cur_max = max();
      if (r < cur_min)
        r = static_cast<double>(cur_min);
      if (r > cur_max)
        r = static_cast<double>(cur_max);
      return r;
    }
  }
  return static_cast<double>(max());
}

double Histogram::Average() const {
  uint64_t cur_num = num();
  uint64_t cur_sum = sum();
  if (cur_num == 0)
    return 0;
  return static_cast<double>(cur_sum) / static_cast<double>(cur_num);
}

double Histogram::StandardDeviation() const {
  uint64_t cur_num = num();
  uint64_t cur_sum = sum();
  uint64_t cur_sum_squares = sum_squares();
  if (cur_num == 0)
    return 0;
  double variance =
      static_cast<double>(cur_sum_squares * cur_num - cur_sum * cur_sum) /
      static_cast<double>(cur_num * cur_num);
  return std::sqrt(variance);
}

void Histogram::Data(HistogramData *const data) const {
  assert(data);
  data->median = Median();
  data->percentile95 = Percentile(95);
  data->percentile99 = Percentile(99);
  data->max = static_cast<double>(max());
  data->average = Average();
  data->standard_deviation = StandardDeviation();
  data->count = num();
  data->sum = sum();
  data->min = static_cast<double>(min());
}
