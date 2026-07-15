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
std::cerr << "DEBUG: index_oid=" << plan_->GetIndexOid()
          << " index_name=" << index_info_->name_
          << " table_name=" << index_info_->table_name_
          << " runtime_type=" << typeid(*idx_ptr).name()
          << std::endl;

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
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (!plan_->pred_keys_.empty()) {
    // ---- Point lookup mode ----
    while (rid_cursor_ < rids_.size() && tuple_batch->size() < batch_size) {
      RID rid = rids_[rid_cursor_++];
      auto [meta, tuple] = table_info_->table_->GetTuple(rid);
      if (!meta.is_deleted_) {
        tuple_batch->push_back(std::move(tuple));
        rid_batch->push_back(rid);
      }
    }
    return !tuple_batch->empty();
  }

  // ---- Ordered ascending scan mode ----
  while (index_iter_ != nullptr && !index_iter_->IsEnd() && tuple_batch->size() < batch_size) {
    const auto entry = *(*index_iter_);
    const RID rid = entry.second;
    ++(*index_iter_);

    auto [meta, tuple] = table_info_->table_->GetTuple(rid);
    if (!meta.is_deleted_) {
      tuple_batch->push_back(std::move(tuple));
      rid_batch->push_back(rid);
    }
  }
  return !tuple_batch->empty();
}

}  // namespace bustub