//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.h
//
// Identification: src/include/execution/executors/update_executor.h
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
#include "execution/plans/update_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * UpdateExecutor executes an update on a table.
 * Updated values are always pulled from a child.
 */
class UpdateExecutor : public AbstractExecutor {
 public:
  UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<Tuple> *tuple_batch, std::vector<RID> *rid_batch, size_t batch_size) -> bool override;

  /** @return The output schema for the update */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** Update plan */
  const UpdatePlanNode *plan_;

  /** Table metadata */
  const TableInfo *table_info_;

  /** Child executor */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Ensures the result tuple is produced only once */
  bool has_executed_{false};
};

}  // namespace bustub