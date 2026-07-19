//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.cpp
//
// Identification: src/execution/sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  rids_.clear();
  
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  while (child_executor_->Next(&child_tuples, &child_rids, 1000)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {
      tuples_.push_back(child_tuples[i]);
      rids_.push_back(child_rids[i]);
    }
    child_tuples.clear();
    child_rids.clear();
  }

  std::vector<size_t> indices(tuples_.size());
  for (size_t i = 0; i < indices.size(); i++) indices[i] = i;

  auto schema = plan_->OutputSchema();
  auto order_bys = plan_->GetOrderBy();

  std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
    for (const auto &order_by : order_bys) {
      auto type = std::get<0>(order_by);
      auto expr = std::get<2>(order_by);
      auto val_a = expr->Evaluate(&tuples_[a], child_executor_->GetOutputSchema());
      auto val_b = expr->Evaluate(&tuples_[b], child_executor_->GetOutputSchema());
      if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
        continue;
      }
      if (type == OrderByType::DEFAULT || type == OrderByType::ASC) {
        return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
      } else {
        return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
      }
    }
    return false;
  });

  std::vector<Tuple> sorted_tuples;
  std::vector<RID> sorted_rids;
  for (size_t idx : indices) {
    sorted_tuples.push_back(tuples_[idx]);
    sorted_rids.push_back(rids_[idx]);
  }
  tuples_ = std::move(sorted_tuples);
  rids_ = std::move(sorted_rids);
  iter_ = 0;
}

auto SortExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
    -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  while (iter_ < tuples_.size() && tuple_batch->size() < batch_size) {
    tuple_batch->push_back(tuples_[iter_]);
    rid_batch->push_back(rids_[iter_]);
    iter_++;
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
