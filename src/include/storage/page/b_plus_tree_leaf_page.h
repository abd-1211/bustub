//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.h
//
// Identification: src/include/storage/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_DEFAULT_TOMB_CNT 0
#define LEAF_PAGE_TOMB_CNT ((NumTombs < 0) ? LEAF_PAGE_DEFAULT_TOMB_CNT : NumTombs)
#define LEAF_PAGE_SLOT_CNT                                                                               \
  ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE - sizeof(size_t) - (LEAF_PAGE_TOMB_CNT * sizeof(size_t))) / \
   (sizeof(KeyType) + sizeof(ValueType)))  // NOLINT

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf pages also contain a fixed buffer of "tombstone" indexes for entries
 * that have been deleted.
 *
 * Leaf page format (keys are stored in order, tomb order is up to you):
 *  --------------------
 * | HEADER | TOMB_SIZE | (where TOMB_SIZE is num_tombstones_)
 *  --------------------
 *  -----------------------------------
 * | TOMB(0) | TOMB(1) | ... | TOMB(k) |
 *  -----------------------------------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  auto GetTombstones() const -> std::vector<KeyType>;

  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);        // to set the location of a key
  auto ValueAt(int index) const -> ValueType;          // to get the value of a key at an index
  void SetValueAt(int index, const ValueType &value);  // to set the value of a key at a given index
  auto GetMinSize() const -> int { return GetMaxSize() / 2; }
  auto IsTombstoneFull() const -> bool {
    return NumTombs > 0 && num_tombstones_ >= static_cast<size_t>(LEAF_PAGE_TOMB_CNT);
  }
  auto GetNumTombstones() const -> size_t { return num_tombstones_; }

  /** Mark key_array_[index] as logically deleted. Caller must check !IsTombstoneFull() first. */
  void InsertTombstone(int index) { tombstones_[num_tombstones_++] = static_cast<size_t>(index); }
  void ClearTombstones() { num_tombstones_ = 0; }
  /** Remove the tombstone marking key_array_[index] as deleted, if present. */
  void RemoveTombstoneAt(int index) {
    for (size_t i = 0; i < num_tombstones_; i++) {
      if (tombstones_[i] == static_cast<size_t>(index)) {
        for (size_t j = i; j + 1 < num_tombstones_; j++) {
          tombstones_[j] = tombstones_[j + 1];
        }
        num_tombstones_--;
        return;
      }
    }
  }
  auto IsIndexTombstoned(int index) const -> bool {
    for (size_t i = 0; i < num_tombstones_; i++) {
      if (tombstones_[i] == static_cast<size_t>(index)) {
        return true;
      }
    }
    return false;
  }

  /** @return the physical index of the oldest (FIFO-front) tombstone. Caller must ensure num_tombstones_ > 0. */
  auto GetOldestTombstoneIndex() const -> int { return static_cast<int>(tombstones_[0]); }

  /** Remove the oldest tombstone, preserving relative order of the rest. */
  void PopOldestTombstone() {
    for (size_t i = 0; i + 1 < num_tombstones_; i++) {
      tombstones_[i] = tombstones_[i + 1];
    }
    num_tombstones_--;
  }

  /** Call after physically erasing key_array_[pos] (array already shifted left) to keep tombstone indices valid. */
  void ShiftTombstonesAfterErase(int pos) {
    for (size_t i = 0; i < num_tombstones_; i++) {
      if (tombstones_[i] > static_cast<size_t>(pos)) {
        tombstones_[i]--;
      }
    }
  }

  /** Tombstone indices in FIFO (oldest-first) order. */
  auto GetTombstoneIndicesInOrder() const -> std::vector<size_t> {
    return std::vector<size_t>(tombstones_, tombstones_ + num_tombstones_);
  }

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(tombkey1, tombkey2, ...|key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    auto tombs = GetTombstones();
    for (size_t i = 0; i < tombs.size(); i++) {
      kstr.append(std::to_string(tombs[i].ToString()));
      if ((i + 1) < tombs.size()) {
        kstr.append(",");
      }
    }

    kstr.append("|");

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  size_t num_tombstones_;
  // Fixed-size tombstone buffer (indexes into key_array_ / rid_array_).
  size_t tombstones_[LEAF_PAGE_TOMB_CNT];
  // Array members for page data.
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];
  // (Spring 2025) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
