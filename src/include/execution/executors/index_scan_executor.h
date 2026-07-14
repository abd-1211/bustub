//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

 private:
  /**
   * Type of the iterator returned by tree_->GetBeginIterator(), derived via decltype so it
   * always matches regardless of the tree's NumTombs template parameter.
   */
  using IndexIteratorType = decltype(std::declval<BPlusTreeIndexForTwoIntegerColumn>().GetBeginIterator());

  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  /** The table that the index is built on. */
  std::shared_ptr<TableInfo> table_info_{nullptr};

  /** The index's metadata / handle. */
  std::shared_ptr<IndexInfo> index_info_{nullptr};

  /** The underlying B+ tree, cast down from index_info_->index_. */
  BPlusTreeIndexForTwoIntegerColumn *tree_{nullptr};

  /**
   * RIDs resolved via point lookup(s) (populated in Init() when plan_->pred_keys_ is non-empty).
   * Supports several point lookups on the same index by concatenating the ScanKey()
   * results for every key in plan_->pred_keys_.
   */
  std::vector<RID> rids_;

  /** Cursor into rids_, used only in point-lookup mode. */
  size_t rid_cursor_{0};

  /**
   * Ascending iterator over the whole index, used only when there is no point-lookup
   * predicate (i.e. this IndexScan implements an ORDER BY on the indexed column).
   *
   * NOTE: IndexIterator holds a ReadPageGuard and has a user-declared destructor, so it has
   * no move constructor (implicitly suppressed) and no accessible copy constructor (the
   * ReadPageGuard member is move-only) -- it is neither copyable nor movable. std::optional
   * can't hold it (emplace()/converting-ctor forward through a reference parameter, which
   * requires an actual move). std::unique_ptr works because `new IndexIteratorType(expr)`,
   * where expr is itself a prvalue of type IndexIteratorType, is one of the few contexts
   * where C++17 guaranteed copy elision still applies -- no copy/move constructor is ever
   * invoked; the returned iterator is constructed directly in the newly allocated storage.
   */
  std::unique_ptr<IndexIteratorType> index_iter_;
};
}  // namespace bustub