//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();

  table_info_ = catalog->GetTable(plan_->table_oid_);
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  Index *idx_ptr = index_info_->index_.get();


tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(idx_ptr);
  BUSTUB_ASSERT(tree_ != nullptr, "IndexScanExecutor: expected BPlusTreeIndexForTwoIntegerColumn");

  // Reset scan state -- Init() may be called more than once (e.g. re-executed queries).
  rids_.clear();
  rid_cursor_ = 0;
  index_iter_.reset();

  if (!plan_->pred_keys_.empty()) {
    // ---- Point lookup mode: WHERE <index column> = <val> (possibly several such lookups) ----
    for (const auto &pred_key : plan_->pred_keys_) {
      // pred_key is a constant-value expression; it doesn't depend on any input tuple.
      Value key_val = pred_key->Evaluate(nullptr, plan_->OutputSchema());
      Tuple key_tuple(std::vector<Value>{key_val}, index_info_->index_->GetKeySchema());

      std::vector<RID> result;
      tree_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
      for (const auto &rid : result) {
        rids_.push_back(rid);
      }
    }
    return;
  }

  // ---- Ordered scan mode: SELECT ... ORDER BY <index column> (ascending only) ----
  // `new IndexIteratorType(prvalue)` triggers guaranteed copy elision (the returned iterator
  // is constructed directly in the new heap storage), so this compiles even though
  // IndexIteratorType has no copy/move constructor.
  index_iter_.reset(new IndexIteratorType(tree_->GetBeginIterator()));

  auto *txn = exec_ctx_->GetTransaction();
  if (txn != nullptr && txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (plan_->filter_predicate_ != nullptr) {
      txn->AppendScanPredicate(table_info_->oid_, plan_->filter_predicate_);
    } else {
      txn->AppendScanPredicate(table_info_->oid_, std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)));
    }
  }
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  if (!plan_->pred_keys_.empty()) {
    // ---- Point lookup mode ----
    while (rid_cursor_ < rids_.size() && tuple_batch->size() < batch_size) {
      RID rid = rids_[rid_cursor_++];
      
      if (txn == nullptr || txn_mgr == nullptr) {
        auto [meta, tuple] = table_info_->table_->GetTuple(rid);
        if (!meta.is_deleted_) {
          tuple_batch->push_back(std::move(tuple));
          rid_batch->push_back(rid);
        }
        continue;
      }

      auto tuple_link = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);
      auto base_meta = std::get<0>(tuple_link);
      auto base_tuple = std::get<1>(tuple_link);
      auto undo_link = std::get<2>(tuple_link);

      auto undo_logs = CollectUndoLogs(rid, base_meta, base_tuple, undo_link, txn, txn_mgr);
      if (undo_logs.has_value()) {
        auto reconstructed = ReconstructTuple(&table_info_->schema_, base_tuple, base_meta, *undo_logs);
        if (reconstructed.has_value()) {
          tuple_batch->push_back(*reconstructed);
          rid_batch->push_back(rid);
        }
      }
    }
    return !tuple_batch->empty();
  }

  // ---- Ordered ascending scan mode ----
  while (index_iter_ != nullptr && !index_iter_->IsEnd() && tuple_batch->size() < batch_size) {
    const auto entry = *(*index_iter_);
    const RID rid = entry.second;
    ++(*index_iter_);

    if (txn == nullptr || txn_mgr == nullptr) {
      auto [meta, tuple] = table_info_->table_->GetTuple(rid);
      if (!meta.is_deleted_) {
        tuple_batch->push_back(std::move(tuple));
        rid_batch->push_back(rid);
      }
      continue;
    }

    auto tuple_link = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);
    auto base_meta = std::get<0>(tuple_link);
    auto base_tuple = std::get<1>(tuple_link);
    auto undo_link = std::get<2>(tuple_link);

    auto undo_logs = CollectUndoLogs(rid, base_meta, base_tuple, undo_link, txn, txn_mgr);
    if (undo_logs.has_value()) {
      auto reconstructed = ReconstructTuple(&table_info_->schema_, base_tuple, base_meta, *undo_logs);
      if (reconstructed.has_value()) {
        tuple_batch->push_back(*reconstructed);
        rid_batch->push_back(rid);
      }
    }
  }
  return !tuple_batch->empty();
}

}  // namespace bustub