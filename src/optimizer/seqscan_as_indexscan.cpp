//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

namespace {

/**
 * @brief Try to interpret `expr` as a point-lookup predicate on a single column: either
 * a bare equality (`col = const` or `const = col`), or an OR-chain of such equalities that
 * all reference the *same* column.
 *
 * On success, appends the constant-value expression(s) found to `keys` and records/checks
 * the column index against `col_idx`. Returns false (and may leave `keys`/`col_idx` partially
 * populated) for any unsupported shape -- e.g. AND, non-equality comparisons, an OR mixing
 * different columns, or an OR with a non-equality/non-single-column child. Callers should
 * treat a `false` return as "give up, fall back to seq scan", not attempt to salvage `keys`.
 */
auto ExtractPointLookupKeys(const AbstractExpressionRef &expr, std::optional<uint32_t> *col_idx,
                            std::vector<AbstractExpressionRef> *keys) -> bool {
  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); cmp_expr != nullptr) {
    if (cmp_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }

    const auto &lhs = cmp_expr->GetChildAt(0);
    const auto &rhs = cmp_expr->GetChildAt(1);

    const auto *lhs_col = dynamic_cast<const ColumnValueExpression *>(lhs.get());
    const auto *rhs_col = dynamic_cast<const ColumnValueExpression *>(rhs.get());
    const auto *lhs_const = dynamic_cast<const ConstantValueExpression *>(lhs.get());
    const auto *rhs_const = dynamic_cast<const ConstantValueExpression *>(rhs.get());

    const ColumnValueExpression *col_expr = nullptr;
    AbstractExpressionRef const_expr_ref = nullptr;

    if (lhs_col != nullptr && rhs_const != nullptr) {
      // col = const
      col_expr = lhs_col;
      const_expr_ref = rhs;
    } else if (rhs_col != nullptr && lhs_const != nullptr) {
      // const = col
      col_expr = rhs_col;
      const_expr_ref = lhs;
    } else {
      // Not a column-vs-constant equality (e.g. col = col, or const = const).
      return false;
    }

    if (col_expr->GetTupleIdx() != 0) {
      // SeqScan has a single input side; anything else shouldn't happen here, but be safe.
      return false;
    }

    if (col_idx->has_value()) {
      if (col_idx->value() != col_expr->GetColIdx()) {
        // OR-chain referencing more than one column -- unsupported, bail out.
        return false;
      }
    } else {
      *col_idx = col_expr->GetColIdx();
    }

    keys->push_back(const_expr_ref);
    return true;
  }

  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); logic_expr != nullptr) {
    if (logic_expr->logic_type_ != LogicType::Or) {
      // AND (or anything else): queries like `v1 = 1 AND v2 = 2` must stay a seq scan.
      return false;
    }
    return ExtractPointLookupKeys(expr->GetChildAt(0), col_idx, keys) &&
          ExtractPointLookupKeys(expr->GetChildAt(1), col_idx, keys);
  }

  return false;
}

}  // namespace

/**
 * @brief transform SeqScanPlanNode into IndexScanPlanNode when the (already pushed-down)
 * filter predicate is a point-lookup pattern -- a single equality, or an OR of equalities on
 * the same column -- and an index exists on that column.
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  if (seq_scan_plan.filter_predicate_ == nullptr) {
    return optimized_plan;
  }

  std::optional<uint32_t> col_idx;
  std::vector<AbstractExpressionRef> raw_keys;
  if (!ExtractPointLookupKeys(seq_scan_plan.filter_predicate_, &col_idx, &raw_keys) || !col_idx.has_value()) {
    return optimized_plan;
  }

  auto index_match = MatchIndex(seq_scan_plan.table_name_, *col_idx);
  if (!index_match.has_value()) {
    return optimized_plan;
  }
  auto [index_oid, index_name] = *index_match;

  // De-duplicate keys (e.g. `WHERE v1 = 1 OR v1 = 1`) so we don't ScanKey() the same value
  // twice and emit duplicate tuples. Dedup by the constant's stringified value.
  std::vector<AbstractExpressionRef> dedup_keys;
  std::unordered_set<std::string> seen;
  for (const auto &key : raw_keys) {
    Value val = key->Evaluate(nullptr, seq_scan_plan.OutputSchema());
    if (seen.insert(val.ToString()).second) {
      dedup_keys.push_back(key);
    }
  }

  return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index_oid,
                                             seq_scan_plan.filter_predicate_, std::move(dedup_keys));
}

}  // namespace bustub
