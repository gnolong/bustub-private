//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout << "throught aggregation_executor" << '\n';
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> values;
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    *tuple = {std::move(values), &GetOutputSchema()};
    *rid = RID{};
    ++aht_iterator_;
    return true;
  }
  if (not_first_call_) {
    return false;
  }
  Tuple child_tuple;
  RID rid_t{};
  if (!child_->Next(&child_tuple, &rid_t)) {
    if (!plan_->group_bys_.empty()) {
      return false;
    }
    not_first_call_ = true;
    *tuple = {std::move(aht_.GenerateInitialAggregateValue().aggregates_), &GetOutputSchema()};
    *rid = RID{};
    return true;
  }
  not_first_call_ = true;
  do {
    // std::cout << "  aggregation_child_tuple_rid: " << rid_t.ToString();
    // std::cout << "  aggregation_child_tuple: " << child_tuple.ToString(&child_->GetOutputSchema()) << '\n';
    auto key = MakeAggregateKey(&child_tuple);
    auto agg_val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, agg_val);
  } while (child_->Next(&child_tuple, &rid_t));
  aht_iterator_ = aht_.Begin();
  std::cout << "  aggregation child out schema" << child_->GetOutputSchema().ToString() << '\n';
  std::cout << "  aggregation out schema" << GetOutputSchema().ToString() << '\n';
  std::vector<Value> values;
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = {std::move(values), &GetOutputSchema()};
  *rid = RID{};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
