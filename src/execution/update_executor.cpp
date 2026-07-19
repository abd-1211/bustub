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

  // Check for primary key index
  IndexInfo *primary_key_index = nullptr;
  for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    if (index_info->is_primary_key_) {
      primary_key_index = index_info.get();
      break;
    }
  }

  std::vector<bool> is_pk_updated_vec(entries.size(), false);
  for (size_t i = 0; i < entries.size(); i++) {
    auto &[old_tuple, rid, new_tuple] = entries[i];
    if (primary_key_index != nullptr) {
      auto *index_meta = primary_key_index->index_->GetMetadata();
      auto old_key = old_tuple.KeyFromTuple(schema, *index_meta->GetKeySchema(), index_meta->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(schema, *index_meta->GetKeySchema(), index_meta->GetKeyAttrs());
      
      for (uint32_t j = 0; j < index_meta->GetKeyAttrs().size(); j++) {
        if (old_key.GetValue(index_meta->GetKeySchema(), j).CompareEquals(new_key.GetValue(index_meta->GetKeySchema(), j)) != CmpBool::CmpTrue) {
          is_pk_updated_vec[i] = true;
          break;
        }
      }
    }

    if (is_pk_updated_vec[i]) {
      // Primary key update: 1. Delete old tuple
      bool del_success = false;
      while (!del_success) {
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

        del_success = UpdateTupleAndUndoLink(txn_mgr, rid, next_undo_link, table_info_->table_.get(), txn, TupleMeta{temp_ts, true}, base_tuple,
          [txn_mgr, base_meta, curr_link](const TupleMeta &m, const Tuple &t, RID r, std::optional<UndoLink> /*l*/) {
            return m == base_meta && txn_mgr->GetUndoLink(r) == curr_link;
          });
      }
      txn->AppendWriteSet(table_info_->oid_, rid);
    }
  }

  for (size_t i = 0; i < entries.size(); i++) {
    auto &[old_tuple, rid, new_tuple] = entries[i];
    
    if (!is_pk_updated_vec[i]) {
      // Normal in-place update using CAS
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
            auto updated_log = GenerateUpdatedUndoLog(&schema, &base_tuple, &new_tuple, existing_log);
            txn->ModifyUndoLog(curr_link->prev_log_idx_, updated_log);
          }
          next_undo_link = curr_link;
        } else {
          auto undo_log = GenerateNewUndoLog(&schema, &base_tuple, &new_tuple, base_meta.ts_, curr_link.has_value() ? *curr_link : UndoLink{});
          next_undo_link = txn->AppendUndoLog(undo_log);
        }

        success = UpdateTupleAndUndoLink(txn_mgr, rid, next_undo_link, table_info_->table_.get(), txn, TupleMeta{temp_ts, false}, new_tuple,
          [txn_mgr, base_meta, curr_link](const TupleMeta &m, const Tuple &t, RID r, std::optional<UndoLink> /*l*/) {
            return m == base_meta && txn_mgr->GetUndoLink(r) == curr_link;
          });
      }

      txn->AppendWriteSet(table_info_->oid_, rid);
      updated_rows++;
    } else {
      // Primary key update: 2. Insert new tuple
      bool inserted_via_update = false;
      auto *index_meta = primary_key_index->index_->GetMetadata();
      auto key = new_tuple.KeyFromTuple(schema, *index_meta->GetKeySchema(), index_meta->GetKeyAttrs());

      std::vector<RID> result;
      primary_key_index->index_->ScanKey(key, &result, txn);

      if (!result.empty()) {
        RID existing_rid = result[0];
        auto tuple_link = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), existing_rid);
        auto base_meta = std::get<0>(tuple_link);

        if (!base_meta.is_deleted_) {
          txn->SetTainted();
          throw ExecutionException("duplicate key violation: primary key already exists");
        }

        bool ins_success = false;
        while (!ins_success) {
          auto curr_tuple_link = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), existing_rid);
          auto curr_meta = std::get<0>(curr_tuple_link);
          auto curr_link = std::get<2>(curr_tuple_link);

          if (!curr_meta.is_deleted_) {
            txn->SetTainted();
            throw ExecutionException("duplicate key violation during CAS update");
          }

          if (curr_meta.ts_ != temp_ts) {
            if (curr_meta.ts_ >= TXN_START_ID) {
              txn->SetTainted();
              throw ExecutionException("write-write conflict during insert into deleted RID");
            }
            if (curr_meta.ts_ > txn->GetReadTs()) {
              txn->SetTainted();
              throw ExecutionException("write-write conflict during insert into deleted RID");
            }
          }

          std::optional<UndoLink> next_undo_link;
          if (curr_meta.ts_ == temp_ts) {
            if (curr_link.has_value() && curr_link->IsValid() && curr_link->prev_txn_ == txn->GetTransactionId()) {
              auto existing_log = txn->GetUndoLog(curr_link->prev_log_idx_);
              auto updated_log = GenerateUpdatedUndoLog(&schema, nullptr, &new_tuple, existing_log);
              txn->ModifyUndoLog(curr_link->prev_log_idx_, updated_log);
            }
            next_undo_link = curr_link;
          } else {
            auto undo_log = UndoLog{true, std::vector<bool>(schema.GetColumnCount(), false), Tuple{}, curr_meta.ts_, curr_link.has_value() ? *curr_link : UndoLink{}};
            next_undo_link = txn->AppendUndoLog(undo_log);
          }

          ins_success = UpdateTupleAndUndoLink(txn_mgr, existing_rid, next_undo_link, table_info_->table_.get(), txn, TupleMeta{temp_ts, false}, new_tuple,
            [txn_mgr, curr_meta, curr_link](const TupleMeta &m, const Tuple &t, RID r, std::optional<UndoLink> /*l*/) {
              return m == curr_meta && txn_mgr->GetUndoLink(r) == curr_link;
            });
        }
        txn->AppendWriteSet(table_info_->oid_, existing_rid);
        inserted_via_update = true;
      }

      if (!inserted_via_update) {
        auto inserted_rid = table_info_->table_->InsertTuple(TupleMeta{temp_ts, false}, new_tuple, exec_ctx_->GetLockManager(), txn, table_info_->oid_);
        BUSTUB_ENSURE(inserted_rid.has_value(), "UpdateExecutor: failed to insert new tuple");
        txn->AppendWriteSet(table_info_->oid_, inserted_rid.value());

        for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
          auto *idx_meta = index_info->index_->GetMetadata();
          auto idx_key = new_tuple.KeyFromTuple(schema, *idx_meta->GetKeySchema(), idx_meta->GetKeyAttrs());
          if (!index_info->index_->InsertEntry(idx_key, inserted_rid.value(), txn)) {
            txn->SetTainted();
            throw ExecutionException("duplicate key violation during index insertion");
          }
        }
      }

      updated_rows++;
    }
  }

  tuple_batch->emplace_back(std::vector<Value>{Value(TypeId::INTEGER, updated_rows)}, &GetOutputSchema());

  has_executed_ = true;
  return true;
}
}  // namespace bustub
