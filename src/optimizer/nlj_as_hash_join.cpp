//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ExtractEquiConditions(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> *left_exprs,
          std::vector<AbstractExpressionRef> *right_exprs)->bool
{
  if (auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(expr))
  {
    if(logic_expr->logic_type_ != LogicType::And)
    {
      return false;
    }
    return ExtractEquiConditions(logic_expr->GetChildAt(0),left_exprs, right_exprs) &&
           ExtractEquiConditions(logic_expr->GetChildAt(1), left_exprs, right_exprs);

  }

  if(auto cmp_expr = std::dynamic_pointer_cast<ComparisonExpression>(expr))
  {
    if(cmp_expr->comp_type_ != ComparisonType::Equal)
    {
      return false;
    }

    auto lhs_col = std::dynamic_pointer_cast<ColumnValueExpression>(cmp_expr->GetChildAt(0));
    auto rhs_col = std::dynamic_pointer_cast<ColumnValueExpression>(cmp_expr->GetChildAt(1));
    if(lhs_col == nullptr || rhs_col == nullptr)
    {
      return false;
    }

    if(lhs_col->GetTupleIdx() ==0 && rhs_col->GetTupleIdx() == 1)
    {
      left_exprs->push_back(lhs_col);
      right_exprs->push_back(rhs_col);
      return true;
    }                                                                 // assign each expression according to its corresponding subtree

    if(lhs_col->GetTupleIdx() ==1 && rhs_col->GetTupleIdx() == 0) 
    {
      left_exprs->push_back(rhs_col);
      right_exprs->push_back(lhs_col);
      return true;
    }
    return false;
  }
  return false;
}



/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for(const auto &child : plan->GetChildren()) // get every node
  {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if(optimized_plan->GetType() == PlanType::NestedLoopJoin)
  {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    std::vector<AbstractExpressionRef> left_key_exprs;
    std::vector<AbstractExpressionRef> right_key_exprs;

    if(ExtractEquiConditions(nlj_plan.Predicate(),&left_key_exprs,&right_key_exprs))
    {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_,nlj_plan.GetLeftPlan(),nlj_plan.GetRightPlan(),std::move(left_key_exprs),std::move(right_key_exprs),nlj_plan.GetJoinType());
    }
  }


  return optimized_plan;
}



}  // namespace bustub
