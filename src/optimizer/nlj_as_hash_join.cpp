#include <algorithm>
#include <memory>
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

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    //dynamic_cast convert failure will  return nullptr
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            // Ensure both exprs have tuple_id == 0
            // Now it's in form of <column_expr> = <column_expr>. Let's match an index for them.
            std::vector<AbstractExpressionRef> left_exprs;
            std::vector<AbstractExpressionRef> right_exprs;
            if(left_expr->GetTupleIdx() == 0){
              auto left_expr_tuple_0 =
                  std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
              auto right_expr_tuple_0 =
                  std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType());
              left_exprs.push_back(std::move(left_expr_tuple_0));
              right_exprs.push_back(std::move(right_expr_tuple_0));
            }
            else{
              auto left_expr_tuple_0 =
                  std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType());
              auto right_expr_tuple_0 =
                  std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
              right_exprs.push_back(std::move(left_expr_tuple_0));
              left_exprs.push_back(std::move(right_expr_tuple_0));
            }
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                nlj_plan.GetRightPlan(), std::move(left_exprs), std::move(right_exprs),
                nlj_plan.GetJoinType());
            }
          }
        }
    }
    else if(const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr){
      if(expr->logic_type_ == LogicType::And){
        if(const auto *left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
          left_expr != nullptr && left_expr->comp_type_ == ComparisonType::Equal){
          if(const auto *right_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
            right_expr != nullptr && right_expr->comp_type_ == ComparisonType::Equal){
            if (const auto *left_expr1 = dynamic_cast<const ColumnValueExpression *>(left_expr->children_[0].get());
                left_expr1 != nullptr) {
              if (const auto *right_expr1 = dynamic_cast<const ColumnValueExpression *>(left_expr->children_[1].get());
                  right_expr1 != nullptr) {
                    if (const auto *left_expr2 = dynamic_cast<const ColumnValueExpression *>(right_expr->children_[0].get());
                        left_expr2 != nullptr) {
                      if (const auto *right_expr2 = dynamic_cast<const ColumnValueExpression *>(right_expr->children_[1].get());
                          right_expr2 != nullptr) {


            std::vector<AbstractExpressionRef> left_exprs;
            std::vector<AbstractExpressionRef> right_exprs;
            std::vector<std::shared_ptr<ColumnValueExpression>> arr_exprs;
            arr_exprs.push_back(std::make_shared<ColumnValueExpression>(left_expr1->GetTupleIdx(), left_expr1->GetColIdx(), left_expr1->GetReturnType()));
            arr_exprs.push_back(std::make_shared<ColumnValueExpression>(right_expr1->GetTupleIdx(), right_expr1->GetColIdx(), right_expr1->GetReturnType()));
            arr_exprs.push_back(std::make_shared<ColumnValueExpression>(left_expr2->GetTupleIdx(), left_expr2->GetColIdx(), left_expr2->GetReturnType()));
            arr_exprs.push_back(std::make_shared<ColumnValueExpression>(right_expr2->GetTupleIdx(), right_expr2->GetColIdx(), right_expr2->GetReturnType()));
            
            for(auto & expr_t: arr_exprs){
              if(expr_t->GetTupleIdx() == 0){
                left_exprs.push_back(std::move(expr_t));
              }
              else{
                right_exprs.push_back(std::move(expr_t));
              }
            }


            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                nlj_plan.GetRightPlan(), std::move(left_exprs), std::move(right_exprs),
                nlj_plan.GetJoinType());
  
                          }
                      }
                    
                  }
              }
        }
      } 
    }
  }
}
  return optimized_plan;
}

}  // namespace bustub
