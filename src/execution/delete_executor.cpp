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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  const auto &schema = table_info_->schema_;

  int32_t deleted_rows = 0;

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  auto temp_ts = txn->GetTransactionTempTs();

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {
      const RID &rid = child_rids[i];

      // Get current base tuple metadata
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
          // Has existing undo log — update it to account for the delete
          auto [heap_meta, heap_tuple] = table_info_->table_->GetTuple(rid);
          auto existing_log = txn->GetUndoLog(undo_link->prev_log_idx_);
          auto updated_log = GenerateUpdatedUndoLog(&schema, &heap_tuple, nullptr, existing_log);
          txn->ModifyUndoLog(undo_link->prev_log_idx_, updated_log);
        }
        // else: self-inserted tuple (no undo log needed), just mark deleted

        // Mark the tuple as deleted in-place
        table_info_->table_->UpdateTupleMeta(TupleMeta{temp_ts, true}, rid);
      } else {
        // First modification by this txn — create a new undo log
        auto [heap_meta, heap_tuple] = table_info_->table_->GetTuple(rid);
        auto prev_link = txn_mgr->GetUndoLink(rid);
        auto prev_undo_link = prev_link.has_value() ? *prev_link : UndoLink{};

        // For delete: base_tuple is the current tuple, target is nullptr
        auto undo_log = GenerateNewUndoLog(&schema, &heap_tuple, nullptr, base_meta.ts_, prev_undo_link);
        auto new_undo_link = txn->AppendUndoLog(undo_log);

        // Update the undo link to point to our new undo log
        txn_mgr->UpdateUndoLink(rid, new_undo_link);

        // Mark the tuple as deleted
        table_info_->table_->UpdateTupleMeta(TupleMeta{temp_ts, true}, rid);
      }

      // Track in write set
      txn->AppendWriteSet(table_info_->oid_, rid);
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