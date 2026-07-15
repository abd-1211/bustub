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
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  tuple_batch->clear();
  rid_batch->clear();
  while (!table_iter_->IsEnd() && tuple_batch->size() < batch_size) {
    auto [meta, tuple] = table_iter_->GetTuple();
    if (!meta.is_deleted_) {
      bool matches = true;
      if (plan_->filter_predicate_ != nullptr) {
        auto filter_val = plan_->filter_predicate_->Evaluate(&tuple, table_info_->schema_);
        matches = filter_val.GetAs<bool>();
      }
      if (matches) {
        tuple_batch->push_back(tuple);
        rid_batch->push_back(table_iter_->GetRID());
      }
    }

    ++(*table_iter_);
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
