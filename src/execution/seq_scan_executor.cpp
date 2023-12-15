//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
                        AbstractExecutor(exec_ctx),plan_(plan){}

void SeqScanExecutor::Init() {
    itr_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout << "throught seqscan_executor" << '\n';
  while(!itr_->IsEnd() && itr_->GetTuple().first.is_deleted_){
    ++(*itr_);
  }
  if (itr_->IsEnd()) {
    return false;
  }
  *tuple = std::move(itr_->GetTuple().second);
  *rid = itr_->GetRID();
  // std::cout << "  seqscan_up_tuple_rid: " << rid->ToString();
  // std::cout << "  seqscan_up_tuple: " << tuple->ToString(&exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_) << '\n';
  ++(*itr_);
  return true;

}

}  // namespace bustub
