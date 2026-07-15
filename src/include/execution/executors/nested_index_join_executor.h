//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedIndexJoinExecutor executes index join operations.
 */
class NestedIndexJoinExecutor : public AbstractExecutor {
 public:
  NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                          std::unique_ptr<AbstractExecutor> &&child_executor);

  /** @return The output schema for the nested index join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

 private:
  const NestedIndexJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::shared_ptr<TableInfo> inner_table_info_;
  std::shared_ptr<IndexInfo> index_info_;
  BPlusTreeIndexForTwoIntegerColumn *tree_{nullptr};

  std::vector<Tuple> left_batch_;
  size_t left_idx_{0};
  bool left_exhausted_{false};

  auto BuildJoinTuple(const Tuple &left_tuple, const Tuple &right_tuple) -> Tuple;
  auto BuildLeftJoinTuple(const Tuple &left_tuple) -> Tuple;
};
}  // namespace bustub
