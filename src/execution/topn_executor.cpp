#include "execution/executors/topn_executor.h"
#include <functional>
#include <memory>
#include <queue>
#include <vector>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
        itr_(valuess_.rend()){
    }

void TopNExecutor::Init() {
    child_executor_->Init();
    itr_ = valuess_.rbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "throught topn executor" << '\n';
  if(itr_ != valuess_.rend()){
    *tuple = {*itr_, &GetOutputSchema()};
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
  if(!child_executor_->Next(&tuple_t, &rid_t)){
    return false;
  }
  auto schema = plan_->GetChildPlan()->OutputSchema();
  auto len = schema.GetColumnCount();
    auto cmp = [&](PAIR &a, PAIR &b){
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

    };
  std::priority_queue<PAIR, std::vector<PAIR>, decltype(cmp)> heap(cmp);
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
    heap.emplace(std::move(values));
    ++n_;
    if(n_ > plan_->GetN()){
        heap.pop();
        --n_;
    }
  }while(child_executor_->Next(&tuple_t,&rid_t));
  valuess_.reserve(n_);
  while(!heap.empty()){
    valuess_.emplace_back(heap.top().first);
    heap.pop();
  }
  itr_ = valuess_.rbegin();
  if(itr_ == valuess_.rend()){
    return false;
  }
  *tuple = {*itr_, &GetOutputSchema()};
  ++itr_;
  *rid = RID{};
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return n_;
};

}  // namespace bustub
