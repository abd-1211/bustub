//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include <algorithm>
#include <cstddef>
#include <vector>
#include "common/macros.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Construct a new LimitExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The limit plan to be executed
 * @param child_executor The child executor from which limited tuples are pulled
 */
LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the limit */
void LimitExecutor::Init() { 
  //UNIMPLEMENTED("TODO(P3): Add implementation."); 
  child_executor_->Init();
  num_emitted_=0;
}

/**
 * Yield the next tuple batch from the limit.
 * @param[out] tuple_batch The next tuple batch produced by the limit
 * @param[out] rid_batch The next tuple RID batch produced by the limit
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto LimitExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                         size_t batch_size) -> bool {
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
  tuple_batch->clear();
  rid_batch->clear();

  if(num_emitted_ >=plan_->GetLimit())
  {
    return false; // limit hit
  }

  size_t remaining = plan_->GetLimit() -num_emitted_;
  size_t effective_batch_size = std::min(batch_size,remaining); // cant take more than the batch can hold or if the remaining entries are less that then the size we can take that

  std::vector<Tuple> child_batch;
  std::vector<RID> child_rids;
  if(!child_executor_->Next(&child_batch, &child_rids, effective_batch_size))
  {
    return false;
  }
  
  for(size_t i=0; i<child_batch.size();i++)
  {
    tuple_batch->push_back(std::move(child_batch[i]));
    rid_batch->push_back(child_rids[i]);
  }
  num_emitted_+=tuple_batch->size();
  return !tuple_batch->empty();
}

}  // namespace bustub
