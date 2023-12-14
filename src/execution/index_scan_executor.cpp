//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan){}

void IndexScanExecutor::Init() {
    index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
    auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn*>(index_info_->index_.get());
    itr_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    std::cout << "throught indexscan" << '\n';
    auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn*>(index_info_->index_.get());
    if(*itr_ != tree->GetEndIterator()){
        *rid = (**itr_).second;
        *tuple = table_info_->table_->GetTuple(*rid).second;
        ++(*itr_);
        std::cout << "  indexscan_up_tuple_rid: " << rid->ToString();
        std::cout << "  indexscan_up_tuple: " << tuple->ToString(&GetOutputSchema()) << '\n';
        return true;
    }
    return false;
}

}  // namespace bustub
