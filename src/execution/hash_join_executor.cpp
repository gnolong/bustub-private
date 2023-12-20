//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      aht_itr_(aht_.End()),
      itr_(valuess_.end()) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  itr_ = valuess_.begin();
  std::cout << "hashjoin executor plan" << plan_->ToString() << '\n';
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout << "throught hashjoin executor" << '\n';
  if (itr_ != valuess_.end()) {
    *tuple = {*itr_, &GetOutputSchema()};
    ++itr_;
    *rid = RID{};
    return true;
  }
  if (not_first_call_) {
    return false;
  }
  not_first_call_ = true;
  Tuple left_tuple{};
  Tuple right_tuple{};
  RID rid_t{};
  while (!left_child_->Next(&left_tuple, &rid_t)) {
    return false;
  }
  int cnt = 0;
  while (right_child_->Next(&right_tuple, &rid_t)) {
    ++cnt;
    auto key = MakeJoinKey(&right_tuple, JType::right);
    auto val = MakeJoinValue(&right_tuple, JType::right);
    aht_.Insert(key, val);
  }
  if (0 == cnt) {
    return false;
  }
  auto schema_lft = plan_->GetLeftPlan()->OutputSchema();
  auto schema_rt = plan_->GetRightPlan()->OutputSchema();
  if (plan_->GetJoinType() == JoinType::LEFT) {
    std::vector<Value> values_null;
    bool null_done{false};
    do {
      auto len_lft = schema_lft.GetColumnCount();
      auto len_rt = schema_rt.GetColumnCount();
      auto key_left = MakeJoinKey(&left_tuple, JType::left);
      std::vector<Value> values;
      values.reserve(len_lft + len_rt);
      for (uint32_t i = 0; i < len_lft; ++i) {
        values.push_back(left_tuple.GetValue(&schema_lft, i));
      }
      int size = values.size();
      if (aht_.Count(key_left) != 0) {
        auto &valss = aht_[key_left].valuess_;
        for (const auto &vals : valss) {
          values.insert(values.end(), vals.begin(), vals.end());
          valuess_.push_back(values);
          values.resize(size);
        }
      } else {
        if (!null_done) {
          null_done = true;
          for (uint32_t i = 0; i < len_rt; ++i) {
            values_null.push_back(ValueFactory::GetNullValueByType(schema_rt.GetColumn(i).GetType()));
          }
        }
        values.insert(values.end(), values_null.begin(), values_null.end());
        valuess_.push_back(values);
      }
    } while (left_child_->Next(&left_tuple, &rid_t));
  } else if (plan_->GetJoinType() == JoinType::INNER) {
    do {
      auto len_lft = schema_lft.GetColumnCount();
      auto len_rt = schema_rt.GetColumnCount();
      auto key_left = MakeJoinKey(&left_tuple, JType::left);
      std::vector<Value> values;
      values.reserve(len_lft + len_rt);
      for (uint32_t i = 0; i < len_lft; ++i) {
        values.push_back(left_tuple.GetValue(&schema_lft, i));
      }
      int size = values.size();
      if (aht_.Count(key_left) != 0) {
        auto &valss = aht_[key_left].valuess_;
        for (const auto &vals : valss) {
          values.insert(values.end(), vals.begin(), vals.end());
          valuess_.push_back(values);
          values.resize(size);
        }
      }
    } while (left_child_->Next(&left_tuple, &rid_t));
  }
  itr_ = valuess_.begin();
  if (itr_ == valuess_.end()) {
    return false;
  }
  *tuple = {*itr_, &GetOutputSchema()};
  ++itr_;
  *rid = RID{};
  return true;
}

}  // namespace bustub
