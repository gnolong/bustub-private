//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_executor)), right_child_(std::move(right_executor)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
    itr_ = valuess_.begin();
  }
}

void NestedLoopJoinExecutor::Init() {
  // std::cout << "nlj executor init()" << '\n';
  left_child_->Init();
  itr_ = valuess_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "throught nlj executor" << '\n';
  if(itr_ != valuess_.end()){
    *tuple = {*itr_, &GetOutputSchema()};
    ++itr_;
    *rid = RID{};
    return true;
  }
  if(not_first_call_){
    return false;
  }
  not_first_call_ = true;
  Tuple left_tuple{};
  Tuple right_tuple{};
  RID rid_t{};
  if(!left_child_->Next(&left_tuple, &rid_t)){
    return false;
  }
    auto schema_lft = plan_->GetLeftPlan()->OutputSchema();
    auto schema_rt = plan_->GetRightPlan()->OutputSchema();
  if(plan_->GetJoinType() == JoinType::LEFT){
    std::vector<Value> values_null;
    bool null_done{false};
    do{
      std::vector<Value> values_left;
      bool left_done{false};
      auto len_lft = schema_lft.GetColumnCount();
      auto len_rt = schema_rt.GetColumnCount();
      bool joined{false};
      right_child_->Init();
      while(right_child_->Next(&right_tuple, &rid_t)){
        auto val = plan_->Predicate()->EvaluateJoin(&left_tuple, schema_lft, 
                                          &right_tuple, schema_rt);
        if(!val.IsNull() && val.GetAs<bool>()){
          joined = true;
          std::vector<Value> values;
          values.reserve(len_lft + len_rt);
          if(!left_done){
            for(uint32_t i = 0; i < len_lft; ++i){
              values_left.push_back(left_tuple.GetValue(&schema_lft, i));
            left_done = true;
          }
          }
          values.insert(values.end(),values_left.begin(), values_left.end());
          for(uint32_t i = 0; i < len_rt; ++i){
            values.push_back(right_tuple.GetValue(&schema_rt, i));
          }
          valuess_.push_back(std::move(values));
        }                                 
      }
      if(!joined){
        if(!null_done){
          for(uint32_t i = 0; i < len_rt; ++i){
            values_null.push_back(ValueFactory::GetNullValueByType(schema_rt.GetColumn(i).GetType()));
          }
          null_done = true;
        }
        std::vector<Value> values;
        values.reserve(len_lft + len_rt);
        for(uint32_t i = 0; i < len_lft; ++i){
          values.push_back(left_tuple.GetValue(&schema_lft, i));
        }
        values.insert(values.end(),values_null.begin(), values_null.end());
        valuess_.push_back(std::move(values));
      }
    }while(left_child_->Next(&left_tuple, &rid_t));
    right_child_->Init();
  }
  else if(plan_->GetJoinType() == JoinType::INNER){
    do{
      std::vector<Value> values_left;
      bool left_done{false};
      auto len_lft = schema_lft.GetColumnCount();
      auto len_rt = schema_rt.GetColumnCount();
      right_child_->Init();
      while(right_child_->Next(&right_tuple, &rid_t)){
        auto val = plan_->Predicate()->EvaluateJoin(&left_tuple, schema_lft, 
                                          &right_tuple, schema_rt);
        if(!val.IsNull() && val.GetAs<bool>()){
          std::vector<Value> values;
          values.reserve(len_lft + len_rt);
          if(!left_done){
            for(uint32_t i = 0; i < len_lft; ++i){
              values_left.push_back(left_tuple.GetValue(&schema_lft, i));
            left_done = true;
          }
          }
          values.insert(values.end(),values_left.begin(), values_left.end());
          for(uint32_t i = 0; i < len_rt; ++i){
            values.push_back(right_tuple.GetValue(&schema_rt, i));
          }
          valuess_.push_back(std::move(values));
        }                                 
      }
    }while(left_child_->Next(&left_tuple, &rid_t));
    right_child_->Init();
  }
  itr_ = valuess_.begin();
  if(itr_ == valuess_.end()){
    return false;
  }
  *tuple = {*itr_, &GetOutputSchema()};
  ++itr_;
  *rid = RID{};
  return true;
}

}  // namespace bustub
