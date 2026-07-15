//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor) ), aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()), aht_iterator_(aht_.Begin()){}
 
  //UNIMPLEMENTED("TODO(P3): Add implementation.");


/** Initialize the aggregation */
void AggregationExecutor::Init() {
   //UNIMPLEMENTED("TODO(P3): Add implementation."); 
    child_executor_->Init();
    aht_.Clear();
    
    std::vector<Tuple> batch;
    std::vector<RID> rids;
    while(child_executor_->Next(&batch, &rids, BUSTUB_BATCH_SIZE))
    {
      for(auto &tuple : batch)
      {
        aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
      }
    }
    has_run = false;
    aht_iterator_= aht_.Begin();
  }

/**
 * Yield the next tuple batch from the aggregation.
 * @param[out] tuple_batch The next batch of tuples produced by the aggregation
 * @param[out] rid_batch The next batch of tuple RIDs produced by the aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if any tuples were produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                               size_t batch_size) -> bool {
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
  tuple_batch->clear();
  rid_batch->clear();
  // if no group by and table was empty
  if(aht_iterator_ == aht_.End() && !has_run && plan_->GetGroupBys().empty())
  {
    has_run = true;
    std::vector<Value> vals;
    AggregateValue initial = aht_.GenerateInitialAggregateValue();
    for(const auto &val : initial.aggregates_)
    {
      vals.push_back(val);
    }

    tuple_batch->push_back(Tuple(vals,&plan_->OutputSchema()));
    rid_batch->push_back(RID{});
    return true;
  }
  has_run = true;

  while(aht_iterator_ != aht_.End() && tuple_batch->size() < batch_size)
  {
    std::vector<Value> vals;

    for(const auto &val : aht_iterator_.Key().group_bys_)
    {
      vals.push_back(val);
    }
    for(const auto & val : aht_iterator_.Val().aggregates_)
    {
      vals.push_back(val);
    }

    tuple_batch->push_back(Tuple (vals,&plan_->OutputSchema()));
    rid_batch->push_back(RID{});
    ++aht_iterator_;
  }
  return !tuple_batch->empty();
}

/** Do not use or remove this function; otherwise, you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
