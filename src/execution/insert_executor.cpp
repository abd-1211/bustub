//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "type/value_factory.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  BUSTUB_ENSURE(table_info_ != nullptr, "InsertExecutor: target table not found");
  child_executor_->Init();
  is_done_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (is_done_) {
    return false;
  }

  std::vector<bustub::Tuple> child_tuples;
  std::vector<bustub::RID> child_rids;

  auto *txn = exec_ctx_->GetTransaction();

  uint32_t inserted_rows = 0;
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (const auto &tuple : child_tuples) {
      // Use the transaction's temporary timestamp for MVCC
      auto temp_ts = txn->GetTransactionTempTs();
      auto inserted_rid = table_info_->table_->InsertTuple(TupleMeta{temp_ts, false}, tuple, exec_ctx_->GetLockManager(),
                                                           exec_ctx_->GetTransaction(), table_info_->oid_);
      BUSTUB_ENSURE(inserted_rid.has_value(), "InsertExecutor: failed to insert tuple into table heap");

      // Track the RID in the write set so Commit can finalize timestamps
      txn->AppendWriteSet(table_info_->oid_, inserted_rid.value());

      for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        auto *index_meta = index_info->index_->GetMetadata();
        auto key = tuple.KeyFromTuple(table_info_->schema_, *index_meta->GetKeySchema(), index_meta->GetKeyAttrs());
        index_info->index_->InsertEntry(key, inserted_rid.value(), exec_ctx_->GetTransaction());
      }

      inserted_rows++;
    }

    child_tuples.clear();
    child_rids.clear();
  }

  tuple_batch->emplace_back(std::vector<Value>{ValueFactory::GetIntegerValue(static_cast<int32_t>(inserted_rows))},
                            &GetOutputSchema());
  rid_batch->emplace_back(RID{});
  is_done_ = true;
  return true;
}

}  // namespace bustub
