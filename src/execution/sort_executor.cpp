#include "execution/executors/sort_executor.h"
#include <vector>
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)),itr_(valuess_.end()){}

void SortExecutor::Init() {
    child_->Init();
    itr_ = valuess_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "throught sort executor" << '\n';
  if(itr_ != valuess_.end()){
    *tuple = {itr_->first, &GetOutputSchema()};
    ++itr_;
    *rid = RID{};
    return true;
  }
  if(not_first_call_){
    return false;
  }
  not_first_call_ = true;
  Tuple tuple_t{};
  RID rid_t{};
  if(!child_->Next(&tuple_t, &rid_t)){
    return false;
  }
  auto schema = plan_->GetChildPlan()->OutputSchema();
  auto len = schema.GetColumnCount();
  do{
    std::pair<std::vector<Value>,std::vector<Value>> values;
    values.first.reserve(len);
    values.second.reserve(plan_->order_bys_.size());
    for(uint32_t i = 0; i < len; ++i){
        values.first.push_back(tuple_t.GetValue(&schema, i));
    }
    for(auto & order : plan_->order_bys_){
      values.second.emplace_back(order.second->Evaluate(&tuple_t,schema));
    }
    valuess_.emplace_back(std::move(values));
  }while(child_->Next(&tuple_t,&rid_t));
  std::sort(valuess_.begin(), valuess_.end(), [&](std::pair<std::vector<Value>,std::vector<Value>> &a,
      std::pair<std::vector<Value>,std::vector<Value>> &b){
        bool ans = true;
        int len = plan_->order_bys_.size();
        for(int i = 0; i < len; ++i){
          if(CmpBool::CmpTrue == a.second[i].CompareEquals(b.second[i])){
            continue;
          }
          switch (plan_->order_bys_[i].first) {
          case OrderByType::ASC:
          case OrderByType::INVALID:
          case OrderByType::DEFAULT:
            return static_cast<bool>(a.second[i].CompareLessThan(b.second[i]));
          case OrderByType::DESC:
            return static_cast<bool>(a.second[i].CompareGreaterThan(b.second[i]));
          }
        }
        return ans;
        
  });
  itr_ = valuess_.begin();
  if(itr_ == valuess_.end()){
    return false;
  }
  *tuple = {itr_->first, &GetOutputSchema()};
  ++itr_;
  *rid = RID{};
  return true;
}

}  // namespace bustub
