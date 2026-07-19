//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();

  // read timestamp = commit timestamp of the most recently committed transaction
  txn_ref->read_ts_ = last_commit_ts_.load();

  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // acquire a monotonically-increasing commit timestamp
  auto commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // Iterate through the write set and update tuple timestamps to commit ts
  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      auto meta = table_info->table_->GetTupleMeta(rid);
      meta.ts_ = commit_ts;
      table_info->table_->UpdateTupleMeta(meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;
  last_commit_ts_.store(commit_ts);

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(P4): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // Get the watermark = lowest read_ts among all active (uncommitted) transactions.
  auto watermark = GetWatermark();

  // Collect the set of transaction IDs that still have accessible undo logs.
  std::unordered_set<txn_id_t> accessible_txns;

  // Iterate over all tables in the catalog to find all version chains.
  auto table_names = catalog_->GetTableNames();
  for (const auto &table_name : table_names) {
    auto table_info = catalog_->GetTable(table_name);
    auto iter = table_info->table_->MakeIterator();

    while (!iter.IsEnd()) {
      auto rid = iter.GetRID();
      auto [meta, tuple] = iter.GetTuple();

      // Walk the version chain for this tuple
      auto undo_link = GetUndoLink(rid);
      timestamp_t current_ts = meta.ts_;

      // If the base tuple has a temp ts (uncommitted), the txn is still running.
      // We don't GC running transactions anyway, but mark them accessible for safety.
      if (current_ts >= TXN_START_ID) {
        accessible_txns.insert(current_ts);  // temp ts IS the txn id
      }

      while (undo_link.has_value() && undo_link->IsValid()) {
        // If the current version is already visible to all active transactions,
        // no one needs to look at the older versions in the undo logs.
        if (current_ts <= watermark) {
          break;
        }

        auto log_opt = GetUndoLogOptional(*undo_link);
        if (!log_opt.has_value()) {
          break;
        }

        accessible_txns.insert(undo_link->prev_txn_);

        current_ts = log_opt->ts_;
        undo_link = log_opt->prev_version_;
      }

      ++iter;
    }
  }

  // Now remove transactions that are not accessible and are committed/aborted.
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  std::vector<txn_id_t> to_remove;

  for (const auto &[txn_id, txn] : txn_map_) {
    auto state = txn->GetTransactionState();
    if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
      continue;  // Don't GC running or tainted transactions
    }
    if (accessible_txns.find(txn_id) == accessible_txns.end()) {
      to_remove.push_back(txn_id);
    }
  }

  std::string acc_str;
  for (auto t : accessible_txns) acc_str += std::to_string(t) + ",";
  std::string rem_str;
  for (auto t : to_remove) rem_str += std::to_string(t) + ",";
  fmt::println(stderr, "GC Debug: Watermark: {}, Accessible: {}, Removing: {}", watermark, acc_str, rem_str);

  for (auto txn_id : to_remove) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
