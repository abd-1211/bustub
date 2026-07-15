//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>
#include <cstddef>
#include <memory>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, ReadPageGuard leaf_guard, int curr_idx)
    : bpm_(std::move(bpm)), leaf_guard_(std::move(leaf_guard)), curr_idx_(curr_idx) {
  SkipTombstones();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  return bpm_ == nullptr;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  auto leaf = leaf_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
  curr_key_ = leaf->KeyAt(curr_idx_);
  curr_val_ = leaf->ValueAt(curr_idx_);
  return {curr_key_, curr_val_};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");

  curr_idx_++;

  // tombstone check
  SkipTombstones();

  return *this;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::SkipTombstones() {
  while (bpm_ != nullptr) {
    auto leaf = leaf_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
    if (curr_idx_ >= leaf->GetSize()) {
      page_id_t next_id = leaf->GetNextPageId();
      if (next_id == INVALID_PAGE_ID) {
        bpm_ = nullptr;
        leaf_guard_ = ReadPageGuard();
        return;
      }
      leaf_guard_ = bpm_->ReadPage(next_id);
      curr_idx_ = 0;
      continue;
    }
    if (NumTombs > 0 && leaf->IsIndexTombstoned(curr_idx_)) {
      curr_idx_++;
      continue;
    }
    return;
  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
