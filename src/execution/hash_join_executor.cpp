//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <tuple>
#include <unordered_map>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),plan_(plan),left_child_(std::move(left_child)),right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

auto HashJoinExecutor::MakeLeftJoinKey(const Tuple *tuple) ->HashJoinKey
{
  std::vector<Value> keys;
  for(const auto &expr : plan_->LeftJoinKeyExpressions())
  {
    keys.push_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
  }
  return {keys};
}

auto HashJoinExecutor::MakeRightJoinKey(const Tuple *tuple) -> HashJoinKey
{
  std::vector<Value> keys;
  for(const auto &expr : plan_->RightJoinKeyExpressions())
  {
    keys.push_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
  }
  return {keys};
}

/** Initialize the join */
void HashJoinExecutor::Init() { 
  //UNIMPLEMENTED("TODO(P3): Add implementation."); 
  left_child_->Init();
  right_child_->Init();

  output_tuples_.clear();
  output_idx_=0;

  // materialize the right table side into an in memory hash table
  std::unordered_map<HashJoinKey, std::vector<Tuple>> right_ht;
  std::vector<Tuple> right_batch;
  std::vector<RID> right_rids;
  while(right_child_->Next(&right_batch, &right_rids, BUSTUB_BATCH_SIZE))
  {
    for(auto &tuple : right_batch)
    {
      HashJoinKey key = MakeRightJoinKey(&tuple); // make keys for all the right table tuples
      right_ht[key].push_back(tuple); // store the tuples at their key index
    }
  }

  //probe phase : stream the left side through the hash table
  std::vector<Tuple> left_batch;
  std::vector<RID> left_rids;
  while(left_child_->Next(&left_batch, &left_rids, BUSTUB_BATCH_SIZE))
  {
    for(auto &left_tuple : left_batch)
    {
      HashJoinKey key = MakeLeftJoinKey(&left_tuple);
      bool has_null_key = false;
      for(auto &v : key.keys_)
      {
        if(v.IsNull()) // check if key is null
        {
          has_null_key = true; 
          break;
        }
      }

      std::unordered_map<HashJoinKey, std::vector<Tuple>>::iterator it;
      if(has_null_key)
      {
        it = right_ht.end(); // if key is null no point searching just bring iterator to end
      }
      else 
      {
        it = right_ht.find(key); // if key is not null find its value
      }
      
      if(it == right_ht.end()) // if this is the last index to iterate on
      {
        if(plan_->GetJoinType() == JoinType::LEFT) // perform a left join and push to output
        {
          output_tuples_.push_back(BuildLeftJoinTuple(left_tuple));
        }
        continue;
      }
      for(const auto &right_tuple : it->second) // it->first = keys, it->second = tuples
      {
        output_tuples_.push_back(BuildJoinTuple(left_tuple, right_tuple));
      }
    }
  }
}

/**
 * Yield the next tuple batch from the hash join.
 * @param[out] tuple_batch The next tuple batch produced by the hash join
 * @param[out] rid_batch The next tuple RID batch produced by the hash join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
  tuple_batch->clear();
  rid_batch->clear();

  while(output_idx_ < output_tuples_.size() && tuple_batch->size() < batch_size) // from start to end of page 
  {
    tuple_batch->push_back(output_tuples_[output_idx_++]); // put the data
    rid_batch->push_back(RID{}); // and rids into the page
  }
  return !tuple_batch->empty();
}

auto HashJoinExecutor::BuildJoinTuple(const Tuple &left_tuple, const Tuple &right_tuple) -> Tuple {
  std::vector<Value> values;
  const auto &left_schema = left_child_->GetOutputSchema();
  const auto &right_schema = right_child_->GetOutputSchema();

  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple.GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    values.push_back(right_tuple.GetValue(&right_schema, i));
  }
  return Tuple(values, &plan_->OutputSchema());
}

auto HashJoinExecutor::BuildLeftJoinTuple(const Tuple &left_tuple) -> Tuple {
  std::vector<Value> values;
  const auto &left_schema = left_child_->GetOutputSchema();
  const auto &right_schema = right_child_->GetOutputSchema();

  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple.GetValue(&left_schema, i));
  }
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
  }
  return Tuple(values, &plan_->OutputSchema());
}

}  // namespace bustub
