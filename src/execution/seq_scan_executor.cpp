//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  plan_ = plan;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  table_iter_.emplace(table_info_->table_->MakeIterator());
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  while (!table_iter_->IsEnd() && tuple_batch->size() < batch_size) {
    auto [meta, tuple] = table_iter_->GetTuple();
    auto rid = table_iter_->GetRID();

    if (txn == nullptr || txn_mgr == nullptr) {
      // Non-MVCC context (e.g. project 3 style execution): ignore undo logs and deletions.
      bool matches = true;
      if (plan_->filter_predicate_ != nullptr) {
        auto filter_val = plan_->filter_predicate_->Evaluate(&tuple, table_info_->schema_);
        matches = filter_val.GetAs<bool>();
      }
      if (matches && !meta.is_deleted_) {
        tuple_batch->push_back(tuple);
        rid_batch->push_back(rid);
      }
      ++(*table_iter_);
      continue;
    }

    auto undo_link = txn_mgr->GetUndoLink(rid);
    auto undo_logs = CollectUndoLogs(rid, meta, tuple, undo_link, txn, txn_mgr);

    if (undo_logs.has_value()) {
      auto reconstructed = ReconstructTuple(&table_info_->schema_, tuple, meta, *undo_logs);
      if (reconstructed.has_value()) {
        bool matches = true;
        if (plan_->filter_predicate_ != nullptr) {
          auto filter_val = plan_->filter_predicate_->Evaluate(&*reconstructed, table_info_->schema_);
          matches = filter_val.GetAs<bool>();
        }
        if (matches) {
          tuple_batch->push_back(*reconstructed);
          rid_batch->push_back(rid);
        }
      }
    }

    ++(*table_iter_);
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
