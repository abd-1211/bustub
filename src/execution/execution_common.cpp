//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool { return false; }

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  return {};
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                       const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }

  bool deleted = base_meta.is_deleted_;
  for (const auto &log : undo_logs) {
    if (log.is_deleted_) {
      deleted = true;
      continue;
    }
    deleted = false;
    std::vector<uint32_t> attrs;
    attrs.reserve(schema->GetColumnCount());
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        attrs.push_back(i);
      }
    }
    auto partial = Schema::CopySchema(schema, attrs);
    uint32_t j = 0;
    for (uint32_t i : attrs) {
      values[i] = log.tuple_.GetValue(&partial, j);
      j++;
    }
  }

  if (deleted) {
    return std::nullopt;
  }
  return Tuple(values, schema);
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                      Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  (void)rid;
  (void)base_tuple;

  auto read_ts = txn->GetReadTs();

  // Case 3 sub-case: the base tuple was most recently modified by the current (uncommitted) transaction.
  // We always see our own latest writes, so no undo logs are needed.
  if (base_meta.ts_ == txn->GetTransactionTempTs()) {
    return std::vector<UndoLog>{};
  }

  std::vector<UndoLog> logs;
  timestamp_t current_ts = base_meta.ts_;
  auto cur = undo_link;
  while (current_ts > read_ts) {
    if (!cur.has_value() || !cur->IsValid()) {
      break;
    }
    auto log = txn_mgr->GetUndoLogOptional(*cur);
    if (!log.has_value()) {
      break;
    }
    logs.push_back(*log);
    current_ts = log->ts_;
    cur = log->prev_version_;
  }

  // If we ran out of history while the version is still newer than our read timestamp, the tuple did not
  // exist (or was not yet visible) at our read timestamp.
  if (current_ts > read_ts) {
    return std::nullopt;
  }
  return logs;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
                TableHeap *table_heap) {
  auto format_ts = [](timestamp_t ts) -> std::string {
    if (ts & TXN_START_ID) {
      return fmt::format("txn{}", ts ^ TXN_START_ID);
    }
    return std::to_string(ts);
  };

  fmt::println(stderr, "debug_hook: {}", info);
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    auto [meta, tuple] = iter.GetTuple();
    auto rid = iter.GetRID();
    fmt::println(stderr, "RID={}/{} ts={} {}tuple={}", rid.GetPageId(), rid.GetSlotNum(), format_ts(meta.ts_),
                 meta.is_deleted_ ? "<del marker> " : "", tuple.ToString(&table_info->schema_));

    auto undo_link = txn_mgr->GetUndoLink(rid);
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto log = txn_mgr->GetUndoLog(*undo_link);
      std::vector<uint32_t> attrs;
      for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
        if (log.modified_fields_[i]) {
          attrs.push_back(i);
        }
      }
      auto partial = Schema::CopySchema(&table_info->schema_, attrs);
      fmt::println(stderr, "  txn{}@{} {}ts={}", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_,
                   log.is_deleted_ ? "<del> " : "", format_ts(log.ts_));
      fmt::println(stderr, "    {}", log.tuple_.ToString(&partial));
      undo_link = log.prev_version_;
    }
    ++iter;
  }
}

}  // namespace bustub
