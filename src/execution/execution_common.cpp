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
  auto col_count = schema->GetColumnCount();

  // Insert case: base_tuple is nullptr, meaning the tuple didn't exist before.
  // The undo log should indicate "this tuple was deleted (didn't exist)" before the insert.
  if (base_tuple == nullptr) {
    return UndoLog{true, std::vector<bool>(col_count, false), Tuple{}, ts, prev_version};
  }

  // Delete case: target_tuple is nullptr, meaning we're deleting the tuple.
  // Store the full base tuple so we can reconstruct it.
  if (target_tuple == nullptr) {
    std::vector<bool> modified_fields(col_count, true);
    std::vector<Value> values;
    values.reserve(col_count);
    for (uint32_t i = 0; i < col_count; i++) {
      values.push_back(base_tuple->GetValue(schema, i));
    }
    std::vector<uint32_t> attrs;
    attrs.reserve(col_count);
    for (uint32_t i = 0; i < col_count; i++) {
      attrs.push_back(i);
    }
    auto partial_schema = Schema::CopySchema(schema, attrs);
    return UndoLog{false, modified_fields, Tuple{values, &partial_schema}, ts, prev_version};
  }

  // Update case: compare columns and build a partial undo log with changed columns.
  std::vector<bool> modified_fields(col_count, false);
  std::vector<uint32_t> attrs;
  std::vector<Value> values;
  for (uint32_t i = 0; i < col_count; i++) {
    auto base_val = base_tuple->GetValue(schema, i);
    auto target_val = target_tuple->GetValue(schema, i);
    if (!base_val.CompareExactlyEquals(target_val)) {
      modified_fields[i] = true;
      attrs.push_back(i);
      values.push_back(base_val);
    }
  }
  auto partial_schema = Schema::CopySchema(schema, attrs);
  return UndoLog{false, modified_fields, Tuple{values, &partial_schema}, ts, prev_version};
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
  auto col_count = schema->GetColumnCount();

  // If this is a delete (target_tuple == nullptr), the undo log must capture the full
  // pre-transaction state. We need to store ALL columns. For columns that were already
  // tracked in the existing undo log, use those original values. For columns NOT in the
  // existing log, use the current base_tuple values (which this txn has been modifying).
  if (target_tuple == nullptr) {
    // If the original undo log was already a deletion marker (i.e., we inserted then delete),
    // just keep it as a deletion marker.
    if (log.is_deleted_) {
      return UndoLog{true, log.modified_fields_, log.tuple_, log.ts_, log.prev_version_};
    }

    std::vector<bool> modified_fields(col_count, true);
    std::vector<Value> values;
    values.reserve(col_count);

    // Build partial schema for existing log to extract its values
    std::vector<uint32_t> old_attrs;
    for (uint32_t i = 0; i < col_count; i++) {
      if (log.modified_fields_[i]) {
        old_attrs.push_back(i);
      }
    }
    auto old_partial_schema = Schema::CopySchema(schema, old_attrs);

    uint32_t old_j = 0;
    for (uint32_t i = 0; i < col_count; i++) {
      if (log.modified_fields_[i]) {
        // Use the value from the existing undo log (the original pre-txn value)
        values.push_back(log.tuple_.GetValue(&old_partial_schema, old_j));
        old_j++;
      } else {
        // This column was not tracked yet. The base_tuple has our latest in-txn value,
        // which for unmodified columns IS the original value.
        BUSTUB_ASSERT(base_tuple != nullptr, "base_tuple should not be null for delete on modified tuple");
        values.push_back(base_tuple->GetValue(schema, i));
      }
    }

    std::vector<uint32_t> all_attrs;
    all_attrs.reserve(col_count);
    for (uint32_t i = 0; i < col_count; i++) {
      all_attrs.push_back(i);
    }
    auto full_schema = Schema::CopySchema(schema, all_attrs);
    return UndoLog{false, modified_fields, Tuple{values, &full_schema}, log.ts_, log.prev_version_};
  }

  // If this is an insert (base_tuple == nullptr, i.e. we're updating something we deleted,
  // which means "delete then insert" = the tuple existed before, we deleted it, now re-inserting).
  // The undo log should still record the original pre-txn state from the existing log.
  if (base_tuple == nullptr) {
    // We're inserting on top of a deletion. The existing log has the original data.
    // Just keep the existing log as-is (it already records the pre-txn state).
    return UndoLog{log.is_deleted_, log.modified_fields_, log.tuple_, log.ts_, log.prev_version_};
  }

  // Update case: merge new column changes into the existing undo log.
  // For columns changed by the new update: keep the ORIGINAL value from the existing log
  // (or from the base_tuple if not yet tracked).
  // For columns already in the existing log but NOT changed now: keep as-is.

  // Build partial schema for existing log
  std::vector<uint32_t> old_attrs;
  for (uint32_t i = 0; i < col_count; i++) {
    if (log.modified_fields_[i]) {
      old_attrs.push_back(i);
    }
  }
  auto old_partial_schema = Schema::CopySchema(schema, old_attrs);

  // Determine which columns are modified in the new update
  std::vector<bool> new_modified(col_count, false);
  for (uint32_t i = 0; i < col_count; i++) {
    auto base_val = base_tuple->GetValue(schema, i);
    auto target_val = target_tuple->GetValue(schema, i);
    if (!base_val.CompareExactlyEquals(target_val)) {
      new_modified[i] = true;
    }
  }

  // Merge: union of old and new modified fields
  std::vector<bool> merged_modified(col_count, false);
  std::vector<uint32_t> merged_attrs;
  std::vector<Value> merged_values;

  uint32_t old_j = 0;
  for (uint32_t i = 0; i < col_count; i++) {
    if (log.modified_fields_[i] || new_modified[i]) {
      merged_modified[i] = true;
      merged_attrs.push_back(i);
      if (log.modified_fields_[i]) {
        // Already tracked in existing log — use the original value from the log
        merged_values.push_back(log.tuple_.GetValue(&old_partial_schema, old_j));
      } else {
        // Newly modified — the base_tuple's current value IS the pre-update value for this txn step,
        // but since this column wasn't changed before in this txn, it's also the original value.
        merged_values.push_back(base_tuple->GetValue(schema, i));
      }
    }
    if (log.modified_fields_[i]) {
      old_j++;
    }
  }

  auto merged_schema = Schema::CopySchema(schema, merged_attrs);
  return UndoLog{log.is_deleted_, merged_modified, Tuple{merged_values, &merged_schema}, log.ts_, log.prev_version_};
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
      auto log_opt = txn_mgr->GetUndoLogOptional(*undo_link);
      if (!log_opt.has_value()) {
        break;
      }
      auto log = *log_opt;
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
