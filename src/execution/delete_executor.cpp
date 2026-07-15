//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "execution/executors/delete_executor.h"
#include "type/value.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid()).get()),
      child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() {
  child_executor_->Init();
  has_executed_ = false;
}

/**
 * Yield the number of rows deleted.
 */
auto DeleteExecutor::Next(std::vector<Tuple> *tuple_batch, std::vector<RID> *rid_batch, size_t batch_size) -> bool {
  if (has_executed_) {
    return false;
  }

  tuple_batch->clear();
  rid_batch->clear();

  int32_t deleted_rows = 0;

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {
      const Tuple &tuple = child_tuples[i];
      const RID &rid = child_rids[i];

      // Mark tuple as deleted
      TupleMeta meta = table_info_->table_->GetTupleMeta(rid);
      meta.ts_ = 0;
      meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(meta, rid);

      // Remove tuple from every index
      for (const auto &index_info : indexes) {
        Tuple key =
            tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

        index_info->index_->DeleteEntry(key, rid, exec_ctx_->GetTransaction());
      }

      deleted_rows++;
    }

    child_tuples.clear();
    child_rids.clear();
  }

  tuple_batch->emplace_back(std::vector<Value>{Value(TypeId::INTEGER, deleted_rows)}, &GetOutputSchema());

  has_executed_ = true;
  return true;
}

}  // namespace bustub