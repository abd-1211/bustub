//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.cpp
//
// Identification: src/execution/window_function_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include "binder/bound_order_by.h"
#include "common/config.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"



namespace bustub {

/**
 * Construct a new WindowFunctionExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The window aggregation plan to be executed
 */
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

namespace {

auto InitialValueForType(WindowFunctionType type) -> Value {
  if (type == WindowFunctionType::CountStarAggregate) {
    return ValueFactory::GetIntegerValue(0);
  }
  return ValueFactory::GetNullValueByType(TypeId::INTEGER);
}

auto CombineOne(const Value &running, const Value &incoming, WindowFunctionType type) -> Value {
  if (type == WindowFunctionType::CountStarAggregate) {
    return running.Add(ValueFactory::GetIntegerValue(1));
  }
  if (incoming.IsNull()) {
    return running;
  }
  if (running.IsNull()) {
    if (type == WindowFunctionType::CountAggregate) {
      return ValueFactory::GetIntegerValue(1);
    }
    return incoming;
  }
  switch (type) {
    case WindowFunctionType::CountAggregate:
      return running.Add(ValueFactory::GetIntegerValue(1));
    case WindowFunctionType::SumAggregate:
      return running.Add(incoming);
    case WindowFunctionType::MinAggregate:
      return running.Min(incoming);
    case WindowFunctionType::MaxAggregate:
      return running.Max(incoming);
    default:
      return running;
  }
}

auto SortKeysEqual(const SortKey &a, const SortKey &b) -> bool {
  for (size_t i = 0; i < a.size(); i++) {
    if (a[i].IsNull() && b[i].IsNull()) {
      continue;
    }
    if (a[i].IsNull() != b[i].IsNull()) {
      return false;
    }
    if (a[i].CompareEquals(b[i]) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

}  // namespace

    
/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() { 
  //throw NotImplementedException("WindowFunctionExecutor is not implemented"); 
  child_executor_->Init();

  output_tuples_.clear();
  output_idx_ = 0;

  const auto &schema = child_executor_->GetOutputSchema();

  //materialize 
  std::vector<Tuple> rows;
  std::vector<Tuple> batch;
  std::vector<RID> rids;
  while (child_executor_->Next(&batch, &rids,  BUSTUB_BATCH_SIZE))
  {
    for(auto &tuple : batch)
    {
      rows.push_back(tuple);
    }
  }

  // every window func has to share the same order by so we just have to check one entry to confirm if global sort needed or not
  std::vector<OrderBy> shared_order_by;
  if(!plan_->window_functions_.empty())
  {
    shared_order_by = plan_->window_functions_.begin()->second.order_by_; // get orderby of the second entry of the unordered pair which is a windowfunction
  }

  if(!shared_order_by.empty())
  {
    TupleComparator cmp(shared_order_by);
    std::vector<SortEntry> entries;
    for(auto &tuple : rows)
    {
      entries.emplace_back(GenerateSortKey(tuple, shared_order_by, schema),tuple);
    }
    std::sort(entries.begin(),entries.end(),cmp);
    rows.clear();
    for(auto &e : entries)
    {
      rows.push_back(e.second);
    }
  }

  std::unordered_map<uint32_t, std::vector<Value>> results;
  for (const auto &entry : plan_->window_functions_)
  {
    uint32_t col_idx = entry.first;
    const auto &wf = entry.second;
    std::vector<Value> col_results(rows.size());

    auto make_partition_key = [&](const Tuple &t)
    {
      std::vector<Value> keys;
      for(const auto &expr : wf.partition_by_)
      {
        keys.push_back(expr->Evaluate(&t, schema));
      }
      return WindowPartitionKey{keys};
    };
    if(wf.type_ == WindowFunctionType::Rank)
    {
      std::unordered_map<WindowPartitionKey, SortKey> last_key;
      std::unordered_map<WindowPartitionKey, uint32_t> last_rank;
      std::unordered_map<WindowPartitionKey, uint32_t> row_count;

      for(size_t i =0;i<rows.size();i++)
      {
        WindowPartitionKey pkey = make_partition_key(rows[i]);
        SortKey okey = GenerateSortKey(rows[i], wf.order_by_, schema);
        row_count[pkey]++;

        auto it = last_key.find(pkey);
        if(it == last_key.end())
        {
          last_key[pkey] = okey;
          last_rank[pkey] = 1;
        }
        else if (!SortKeysEqual(it->second, okey))
        {
          it->second = okey;
          last_rank[pkey] = row_count[pkey];
        }
        col_results[i] = ValueFactory::GetIntegerValue(static_cast<int32_t>(last_rank[pkey]));
        }
      }
      else if (!wf.order_by_.empty()) {
        //running / cummulative case 
        std::unordered_map<WindowPartitionKey, Value> running;
         for (size_t i = 0; i < rows.size(); i++) {
        WindowPartitionKey pkey = make_partition_key(rows[i]);
        Value input_val = wf.function_->Evaluate(&rows[i], schema);
        auto it = running.find(pkey);
        if (it == running.end()) {
          running[pkey] = InitialValueForType(wf.type_);
          it = running.find(pkey);
        }
         it->second = CombineOne(it->second, input_val, wf.type_);
        col_results[i] = it->second;
      }
      }
      else {
       // whole parition case : two passes
       std::unordered_map<WindowPartitionKey, Value> totals;
       std::vector<WindowPartitionKey> row_keys(rows.size());
       for(size_t i=0;i<rows.size();i++)
       {
        row_keys[i]= make_partition_key(rows[i]);
        Value input_val = wf.function_->Evaluate(&rows[i], schema);
        auto it = totals.find(row_keys[i]);
        if(it == totals.end())
        {
          totals[row_keys[i]] = InitialValueForType(wf.type_);
          it = totals.find(row_keys[i]);
        }
        it->second = CombineOne(it->second, input_val, wf.type_);
       }
       for(size_t i=0; i<rows.size(); i++)
       {
        col_results[i] = totals[row_keys[i]];
       }
      }
      results[col_idx] = std::move(col_results);
    }
    // assemble final output table 
    for(size_t i=0;i<rows.size();i++)
    {
          std::vector<Value> values;

      for(size_t col =0;col<plan_->columns_.size();col++)
      {
        if(plan_->window_functions_.count(static_cast<uint32_t>(col))>0)
        {
          values.push_back(results[static_cast<uint32_t>(col)][i]);
        }
        else {
        values.push_back(plan_->columns_[col]->Evaluate(&rows[i], schema));
        }
      }
      output_tuples_.push_back(Tuple(values,&plan_->OutputSchema()));
    }
  }
}
namespace bustub{
/**
 * Yield the next tuple batch from the window aggregation.
 * @param[out] tuple_batch The next tuple batch produced by the window aggregation
 * @param[out] rid_batch The next tuple RID batch produced by the window aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto WindowFunctionExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  while (output_idx_ < output_tuples_.size() && tuple_batch->size() < batch_size) {
    tuple_batch->push_back(output_tuples_[output_idx_++]);
    rid_batch->push_back(RID{});
  }
  return !tuple_batch->empty();
}
}  // namespace bustub
