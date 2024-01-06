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
#include "concurrency/lock_manager.h"
#include "execution/executor_context.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  itr_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
  exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout << "throught seqscan_executor" << '\n';
  exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),LockManager::LockMode::SHARED,plan_->GetTableOid(),itr_->GetRID());
  while (!itr_->IsEnd() && itr_->GetTuple().first.is_deleted_) {
    ++(*itr_);
  exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),LockManager::LockMode::SHARED,plan_->GetTableOid(),itr_->GetRID());
  }
  if (itr_->IsEnd()) {
    return false;
  }
  *tuple = std::move(itr_->GetTuple().second);
  *rid = itr_->GetRID();
  // std::cout << "  seqscan_up_tuple_rid: " << rid->ToString();
  // std::cout << "  seqscan_up_tuple: " <<
  // tuple->ToString(&exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_) << '\n';
  ++(*itr_);
  return true;
}

}  // namespace bustub
