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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  const auto &schema = table_info_->schema_;

  // Phase 1: Materialize all tuples from child (pipeline breaker)
  struct UpdateEntry {
    Tuple old_tuple;
    RID rid;
    Tuple new_tuple;
  };
  std::vector<UpdateEntry> entries;

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

      entries.push_back({old_tuple, child_rids[i], Tuple{values, &schema}});
    }

    child_tuples.clear();
    child_rids.clear();
  }

  // Phase 2: Apply updates with MVCC logic
  int32_t updated_rows = 0;
  auto temp_ts = txn->GetTransactionTempTs();

  for (auto &[old_tuple, rid, new_tuple] : entries) {
    // Get the current base tuple metadata
    auto base_meta = table_info_->table_->GetTupleMeta(rid);

    // Check for write-write conflict
    if (base_meta.ts_ != temp_ts) {
      // Not our own modification — check for conflicts
      if (base_meta.ts_ >= TXN_START_ID) {
        // Another uncommitted transaction owns this tuple
        txn->SetTainted();
        throw ExecutionException("write-write conflict: tuple owned by another uncommitted txn");
      }
      if (base_meta.ts_ > txn->GetReadTs()) {
        // Tuple was committed after our read timestamp
        txn->SetTainted();
        throw ExecutionException("write-write conflict: tuple committed after our read_ts");
      }
    }

    // Self-modification: the current txn already modified this tuple
    if (base_meta.ts_ == temp_ts) {
      // Check if this tuple has an undo log from this transaction
      auto undo_link = txn_mgr->GetUndoLink(rid);
      if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_txn_ == txn->GetTransactionId()) {
        // Has existing undo log — update it via GenerateUpdatedUndoLog
        auto [base_tuple_heap, base_tuple_data] = table_info_->table_->GetTuple(rid);
        auto existing_log = txn->GetUndoLog(undo_link->prev_log_idx_);
        auto updated_log = GenerateUpdatedUndoLog(&schema, &base_tuple_data, &new_tuple, existing_log);
        txn->ModifyUndoLog(undo_link->prev_log_idx_, updated_log);
      }
      // else: self-inserted tuple (no undo log needed), just update in-place

      // Update the tuple in the table heap in-place
      table_info_->table_->UpdateTupleInPlace(TupleMeta{temp_ts, false}, new_tuple, rid, nullptr);
    } else {
      // First modification by this txn — create a new undo log
      auto [base_tuple_heap_meta, base_tuple_data] = table_info_->table_->GetTuple(rid);
      auto prev_link = txn_mgr->GetUndoLink(rid);
      auto prev_undo_link = prev_link.has_value() ? *prev_link : UndoLink{};

      auto undo_log = GenerateNewUndoLog(&schema, &base_tuple_data, &new_tuple, base_meta.ts_, prev_undo_link);
      auto new_undo_link = txn->AppendUndoLog(undo_log);

      // Update the undo link to point to our new undo log
      txn_mgr->UpdateUndoLink(rid, new_undo_link);

      // Update the tuple in the table heap in-place
      table_info_->table_->UpdateTupleInPlace(TupleMeta{temp_ts, false}, new_tuple, rid, nullptr);
    }

    // Track in write set
    txn->AppendWriteSet(table_info_->oid_, rid);
    updated_rows++;
  }

  tuple_batch->emplace_back(std::vector<Value>{Value(TypeId::INTEGER, updated_rows)}, &GetOutputSchema());

  has_executed_ = true;
  return true;
}
}  // namespace bustub
