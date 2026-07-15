//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  left_executor_->Init();
  right_executor_->Init();

  right_tuples_.clear();
  std::vector<Tuple> batch;
  std::vector<RID> rids;
  while (right_executor_->Next(&batch, &rids, BUSTUB_BATCH_SIZE)) {
    for (auto &tuple : batch) {
      right_tuples_.push_back(tuple);
    }
  }

  left_idx_ = 0;
  right_idx_ = 0;
  left_matched_ = false;
  left_batch_.clear();
  left_exhausted_ = false;
}

auto NestedLoopJoinExecutor::BuildLeftJoinTuple(const Tuple &left_tuple) -> Tuple {
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();

  std::vector<Value> vals;
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    vals.push_back(left_tuple.GetValue(&left_schema, i));
  }

  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    vals.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
  }

  return Tuple(vals, &plan_->OutputSchema());
}

auto NestedLoopJoinExecutor::BuildJoinTuple(const Tuple &left_tuple, const Tuple &right_tuple) -> Tuple {
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();
  std::vector<Value> vals;

  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    vals.push_back(left_tuple.GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    vals.push_back(right_tuple.GetValue(&right_schema, i));
  }

  return Tuple(vals, &plan_->OutputSchema());
}

/**
 * Yield the next tuple batch from the join.
 * @param[out] tuple_batch The next tuple batch produced by the join
 * @param[out] rid_batch The next tuple RID batch produced by the join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  tuple_batch->clear();
  rid_batch->clear();
  while (tuple_batch->size() < batch_size) {
    if (left_idx_ >= left_batch_.size())  // new tuples needed current stored have been used
    {
      std::vector<Tuple> new_batch;
      std::vector<RID> new_rids;
      if (!left_executor_->Next(&new_batch, &new_rids, batch_size))  // if no next tuples are available table has ended
      {
        left_exhausted_ = true;
        break;
      }
      left_batch_ = std::move(new_batch);
      left_idx_ = 0;
      right_idx_ = 0;
      left_matched_ = false;
    }
    const Tuple &left_tuple = left_batch_[left_idx_];

    if (right_idx_ >= right_tuples_.size()) {
      if (plan_->GetJoinType() == JoinType::LEFT && !left_matched_) {  // no match so emit left tuples with NULLs
        tuple_batch->push_back(BuildLeftJoinTuple(left_tuple));
        rid_batch->push_back(RID{});
      }
      left_idx_++;
      right_idx_ = 0;
      left_matched_ = false;
      continue;
    }

    const Tuple &right_tuple = right_tuples_[right_idx_++];
    auto val = plan_->Predicate()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                right_executor_->GetOutputSchema());
    if (!val.IsNull() && val.GetAs<bool>()) {
      tuple_batch->push_back(BuildJoinTuple(left_tuple, right_tuple));
      rid_batch->push_back(RID{});
      left_matched_ = true;
    }
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
