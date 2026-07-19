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

      bool success = false;
      while (!success) {
        auto tuple_link = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);
        auto base_meta = std::get<0>(tuple_link);
        auto base_tuple = std::get<1>(tuple_link);
        auto curr_link = std::get<2>(tuple_link);

        if (base_meta.ts_ != temp_ts) {
          if (base_meta.ts_ >= TXN_START_ID) {
            txn->SetTainted();
            throw ExecutionException("write-write conflict: tuple owned by another uncommitted txn");
          }
          if (base_meta.ts_ > txn->GetReadTs()) {
            txn->SetTainted();
            throw ExecutionException("write-write conflict: tuple committed after our read_ts");
          }
        }

        std::optional<UndoLink> next_undo_link;
        if (base_meta.ts_ == temp_ts) {
          if (curr_link.has_value() && curr_link->IsValid() && curr_link->prev_txn_ == txn->GetTransactionId()) {
            auto existing_log = txn->GetUndoLog(curr_link->prev_log_idx_);
            auto updated_log = GenerateUpdatedUndoLog(&schema, &base_tuple, nullptr, existing_log);
            txn->ModifyUndoLog(curr_link->prev_log_idx_, updated_log);
          }
          next_undo_link = curr_link;
        } else {
          auto undo_log = GenerateNewUndoLog(&schema, &base_tuple, nullptr, base_meta.ts_, curr_link.has_value() ? *curr_link : UndoLink{});
          next_undo_link = txn->AppendUndoLog(undo_log);
        }

        success = UpdateTupleAndUndoLink(txn_mgr, rid, next_undo_link, table_info_->table_.get(), txn, TupleMeta{temp_ts, true}, base_tuple,
          [txn_mgr, base_meta, curr_link](const TupleMeta &m, const Tuple &t, RID r, std::optional<UndoLink> /*l*/) {
            return m == base_meta && txn_mgr->GetUndoLink(r) == curr_link;
          });
      }

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