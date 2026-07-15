//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "execution/executors/update_executor.h"
#include "type/value.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid()).get()),
      child_executor_(std::move(child_executor)) {}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
  has_executed_ = false;
}

/**
 * Yield the number of rows updated.
 */
auto UpdateExecutor::Next(std::vector<Tuple> *tuple_batch, std::vector<RID> *rid_batch, size_t batch_size) -> bool {
  if (has_executed_) {
    return false;
  }

  tuple_batch->clear();
  rid_batch->clear();

  // -------------------------------
  // Phase 1: Materialize everything
  // -------------------------------
  std::vector<Tuple> old_tuples;
  std::vector<RID> old_rids;
  std::vector<Tuple> new_tuples;

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {
      const Tuple &old_tuple = child_tuples[i];

      std::vector<Value> values;
      values.reserve(plan_->target_expressions_.size());

      for (const auto &expr : plan_->target_expressions_) {
        values.emplace_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
      }

      old_tuples.push_back(old_tuple);
      old_rids.push_back(child_rids[i]);
      new_tuples.emplace_back(values, &table_info_->schema_);
    }

    child_tuples.clear();
    child_rids.clear();
  }

  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  // -------------------------------
  // Phase 2: Delete all old tuples
  // -------------------------------
  for (size_t i = 0; i < old_tuples.size(); i++) {
    TupleMeta meta = table_info_->table_->GetTupleMeta(old_rids[i]);
    meta.ts_ = 0;
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, old_rids[i]);

    for (const auto &index_info : indexes) {
      Tuple old_key =
          old_tuples[i].KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      index_info->index_->DeleteEntry(old_key, old_rids[i], exec_ctx_->GetTransaction());
    }
  }

  // -------------------------------
  // Phase 3: Insert all new tuples
  // -------------------------------
  int32_t updated_rows = 0;

  for (size_t i = 0; i < new_tuples.size(); i++) {
    TupleMeta meta;
    meta.ts_ = 0;
    meta.is_deleted_ = false;

    auto rid_opt = table_info_->table_->InsertTuple(meta, new_tuples[i]);

    if (!rid_opt.has_value()) {
      continue;
    }

    RID new_rid = rid_opt.value();

    for (const auto &index_info : indexes) {
      Tuple new_key =
          new_tuples[i].KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
    }

    updated_rows++;
  }

  tuple_batch->emplace_back(std::vector<Value>{Value(TypeId::INTEGER, updated_rows)}, &GetOutputSchema());

  has_executed_ = true;
  return true;
}
}  // namespace bustub
