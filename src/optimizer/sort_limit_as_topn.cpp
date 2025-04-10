#include <memory>
#include <vector>
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> childs;
  for (const auto &child : plan->GetChildren()) {
    childs.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(childs);

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &lmt_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    auto child_plan = lmt_plan.GetChildAt(0);
    if (child_plan->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
      return std::make_shared<TopNPlanNode>(lmt_plan.output_schema_, sort_plan.GetChildPlan(), sort_plan.GetOrderBy(),
                                            lmt_plan.limit_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
