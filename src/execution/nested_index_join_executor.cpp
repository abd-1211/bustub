//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

void NestedIndexJoinExecutor::Init() { 
  //UNIMPLEMENTED("TODO(P3): Add implementation."); 
  child_executor_->Init();

  auto *catalog = exec_ctx_->GetCatalog();
  inner_table_info_ = catalog->GetTable(plan_->GetInnerTableOid());
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());

  left_idx_=0;
  left_batch_.clear();
  left_exhausted_ = false;
}


auto NestedIndexJoinExecutor::BuildJoinTuple(const Tuple &left_tuple, const Tuple &right_tuple) -> Tuple {
  std::vector<Value> values;
  const auto &left_schema = child_executor_->GetOutputSchema();
  const auto &right_schema = inner_table_info_->schema_;

  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple.GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    values.push_back(right_tuple.GetValue(&right_schema, i));
  }

  return Tuple(values, &plan_->OutputSchema());
}

auto NestedIndexJoinExecutor::BuildLeftJoinTuple(const Tuple &left_tuple) -> Tuple {
  std::vector<Value> values;
  const auto &left_schema = child_executor_->GetOutputSchema();
  const auto &right_schema = inner_table_info_->schema_;

  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple.GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
  }

  return Tuple(values, &plan_->OutputSchema());
}


auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
  tuple_batch->clear();
  rid_batch->clear();

  while(tuple_batch->size()<batch_size)
  {
    if(left_idx_>= left_batch_.size())
    {
      std::vector<Tuple> new_batch;
      std::vector<RID> new_rids;
      if(!child_executor_->Next(&new_batch, &new_rids, batch_size))
      {
        left_exhausted_ = true;
        break;
      }
      left_batch_ = std::move(new_batch);
      left_idx_=0;
    }
    const Tuple &left_tuple = left_batch_[left_idx_++];

    // Evaluate the join key expression against this left tuple to get the probe value.
    Value key_val = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    Tuple key_tuple(std::vector<Value>{key_val},index_info_->index_->GetKeySchema() );

    std::vector<RID> result_rids;
    tree_->ScanKey(key_tuple, &result_rids, exec_ctx_->GetTransaction());
    
    if(result_rids.empty())
    {
      if(plan_->GetJoinType() == JoinType::LEFT) // no matches on the inner index side
      {
        tuple_batch->push_back(BuildLeftJoinTuple(left_tuple));
        rid_batch->push_back(RID{});
      }
      continue;
    }
    for(const auto &rid : result_rids)
    {
      auto [meta, right_tuple] = inner_table_info_->table_->GetTuple(rid);
      if (meta.is_deleted_)
      {
        continue;
      }
      tuple_batch->push_back(BuildJoinTuple(left_tuple, right_tuple));
      rid_batch->push_back(RID{});
    }
  }
  return !tuple_batch->empty();
}



}  // namespace bustub
