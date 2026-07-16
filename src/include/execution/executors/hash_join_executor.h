//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> keys_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < keys_.size(); i++) {
      if (keys_[i].IsNull() || other.keys_[i].IsNull()) {
        return false;  // NULL never equals anything in a join key
      }
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  std::vector<Tuple> output_tuples_;
  size_t output_idx_{0};

  auto MakeLeftJoinKey(const Tuple *tuple) -> HashJoinKey;
  auto MakeRightJoinKey(const Tuple *tuple) -> HashJoinKey;
  auto BuildJoinTuple(const Tuple &left_tuple, const Tuple &right_tuple) -> Tuple;
  auto BuildLeftJoinTuple(const Tuple &left_tuple) -> Tuple;
};

}  // namespace bustub

namespace std {
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &val : key.keys_) {
      if (!val.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }
};
}  // namespace std
