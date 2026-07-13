//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {  //initialized sketch_ with constructor
  if(width==0 || depth==0)
  {
    throw std::invalid_argument("Width and Depth must be greater than 0");
  }
  sketch_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
  sketch_.emplace_back(width_);
  }
  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  sketch_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    sketch_.emplace_back(width_);  // construct each row in-place
    for (size_t j = 0; j < width_; j++)
      sketch_[i][j].store(other.sketch_[i][j].load(std::memory_order_relaxed));
  }

  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
  //moved resource ownership to new obj
  other.sketch_.clear();
  other.hash_functions_.clear();
  other.width_ = 0;
  other.depth_ = 0;
  //Nullify the source so stale values are not usable after object has been moved
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if(this!=&other)
  {
  this->width_ = other.width_;
  this->depth_ = other.depth_;
  sketch_.clear();
    sketch_.reserve(depth_);
    for (size_t i = 0; i < depth_; i++) {
      sketch_.emplace_back(width_);
      for (size_t j = 0; j < width_; j++)
        sketch_[i][j].store(other.sketch_[i][j].load(std::memory_order_relaxed));
    }
    
    hash_functions_.clear();
    hash_functions_.reserve(depth_);
    for (size_t i = 0; i < depth_; i++) {
      hash_functions_.push_back(this->HashFunction(i));
    }

    // Nullify the source
    other.sketch_.clear();
    other.hash_functions_.clear();
    other.width_ = 0;
    other.depth_ = 0;
  }
  return *this;
  
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */  

  for(size_t i=0; i<depth_;i++) //loop over each hash function
  {
  auto column =hash_functions_[i](item) % width_; //get value of key for ith hashfunction for the item we've selected
  sketch_[i][column].fetch_add(1, std::memory_order_relaxed); //keep count of item inserted at index
  }
} 

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
        for (size_t j = 0; j < width_; j++) {
            sketch_[i][j].fetch_add(other.sketch_[i][j].load(std::memory_order_relaxed),std::memory_order_relaxed);
        }
      }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  
  uint32_t min_count = UINT32_MAX;

  for (size_t i = 0; i < depth_; i++) {
      auto column = hash_functions_[i](item) % width_;
      min_count = std::min(min_count, sketch_[i][column].load(std::memory_order_relaxed));
  }

  return min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  for (auto &row : sketch_){
    for (auto &cell : row){
      cell.store(0, std::memory_order_relaxed);
    }
  }
    
  
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
    std::vector<std::pair<KeyType, uint32_t>> results;

  // Step 1: compute estimated counts
  for (const auto &item : candidates) {
    uint32_t count = Count(item);
    results.emplace_back(item, count);
  }

  // Step 2: sort by count descending
  std::sort(results.begin(), results.end(),
            [](const auto &a, const auto &b) {
              return a.second > b.second;
            });

  // Step 3: limit to k results
  if (results.size() > k) {
    results.resize(k);
  }

  return results;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
