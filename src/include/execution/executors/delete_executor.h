//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.h
//
// Identification: src/include/execution/executors/delete_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/delete_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * DeleteExecutor executes a delete on a table.
 * Deleted values are always pulled from a child.
 */
class DeleteExecutor : public AbstractExecutor {
 public:
  DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<Tuple> *tuple_batch,
            std::vector<RID> *rid_batch,
            size_t batch_size) -> bool override;

  /** @return The output schema for the delete */
  auto GetOutputSchema() const -> const Schema & override {
    return plan_->OutputSchema();
  }

 private:
  /** The delete plan node to be executed */
  const DeletePlanNode *plan_;

  /** Metadata identifying the table to delete from */
  const TableInfo *table_info_;

  /** The child executor that produces tuples/RIDs to delete */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Ensures the delete count is returned only once */
  bool has_executed_{false};
};

}  // namespace bustub