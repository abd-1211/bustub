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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  uint32_t inserted_rows = 0;
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (const auto &tuple : child_tuples) {
      auto temp_ts = txn->GetTransactionTempTs();

      // Check if there is a primary key index
      IndexInfo *primary_key_index = nullptr;
      for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        if (index_info->is_primary_key_) {
          primary_key_index = index_info.get();
          break;
        }
      }

      bool inserted_via_update = false;

      if (primary_key_index != nullptr) {
        auto *index_meta = primary_key_index->index_->GetMetadata();
        auto key = tuple.KeyFromTuple(table_info_->schema_, *index_meta->GetKeySchema(), index_meta->GetKeyAttrs());

        std::vector<RID> result;
        primary_key_index->index_->ScanKey(key, &result, txn);

        if (!result.empty()) {
          // The key already exists in the index.
          // Check if it points to a deleted tuple in the heap.
          RID existing_rid = result[0];
          auto tuple_link = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), existing_rid);
          auto base_meta = std::get<0>(tuple_link);

          if (!base_meta.is_deleted_) {
            // Duplicate key violation
            txn->SetTainted();
            throw ExecutionException("duplicate key violation: primary key already exists");
          }

          // In-place update of the deleted RID
          bool success = false;
          while (!success) {
            auto curr_tuple_link = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), existing_rid);
            auto curr_meta = std::get<0>(curr_tuple_link);
            auto curr_link = std::get<2>(curr_tuple_link);

            if (!curr_meta.is_deleted_) {
              txn->SetTainted();
              throw ExecutionException("duplicate key violation during CAS update");
            }

            // Write-write conflict detection
            if (curr_meta.ts_ != temp_ts) {
              if (curr_meta.ts_ >= TXN_START_ID) {
                txn->SetTainted();
                throw ExecutionException("write-write conflict during insert into deleted RID (owned by uncommitted txn)");
              }
              if (curr_meta.ts_ > txn->GetReadTs()) {
                txn->SetTainted();
                throw ExecutionException("write-write conflict during insert into deleted RID (committed after read_ts)");
              }
            }

            std::optional<UndoLink> next_undo_link;
            if (curr_meta.ts_ == temp_ts) {
              // Self-modification
              if (curr_link.has_value() && curr_link->IsValid() && curr_link->prev_txn_ == txn->GetTransactionId()) {
                auto existing_log = txn->GetUndoLog(curr_link->prev_log_idx_);
                auto updated_log = GenerateUpdatedUndoLog(&table_info_->schema_, nullptr, &tuple, existing_log);
                txn->ModifyUndoLog(curr_link->prev_log_idx_, updated_log);
              }
              next_undo_link = curr_link;
            } else {
              // First modification
              auto undo_log = UndoLog{true, std::vector<bool>(table_info_->schema_.GetColumnCount(), false), Tuple{}, curr_meta.ts_, curr_link.has_value() ? *curr_link : UndoLink{}};
              next_undo_link = txn->AppendUndoLog(undo_log);
            }

            success = UpdateTupleAndUndoLink(txn_mgr, existing_rid, next_undo_link, table_info_->table_.get(), txn, TupleMeta{temp_ts, false}, tuple,
              [txn_mgr, curr_meta, curr_link](const TupleMeta &m, const Tuple &t, RID r, std::optional<UndoLink> /*l*/) {
                return m == curr_meta && txn_mgr->GetUndoLink(r) == curr_link;
              });
          }

          txn->AppendWriteSet(table_info_->oid_, existing_rid);
          inserted_via_update = true;
        }
      }

      if (!inserted_via_update) {
        // Normal insert
        auto inserted_rid = table_info_->table_->InsertTuple(TupleMeta{temp_ts, false}, tuple, exec_ctx_->GetLockManager(),
                                                             exec_ctx_->GetTransaction(), table_info_->oid_);
        BUSTUB_ENSURE(inserted_rid.has_value(), "InsertExecutor: failed to insert tuple into table heap");

        // Track the RID in the write set
        txn->AppendWriteSet(table_info_->oid_, inserted_rid.value());

        // Insert into indexes
        for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
          auto *index_meta = index_info->index_->GetMetadata();
          auto key = tuple.KeyFromTuple(table_info_->schema_, *index_meta->GetKeySchema(), index_meta->GetKeyAttrs());
          if (!index_info->index_->InsertEntry(key, inserted_rid.value(), exec_ctx_->GetTransaction())) {
            txn->SetTainted();
            throw ExecutionException("duplicate key violation during index insertion");
          }
        }
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
