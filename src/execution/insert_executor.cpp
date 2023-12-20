//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // std::cout << "throught insert_executor" << '\n';
  Tuple child_tuple;
  RID rid_t{};
  int s = 0;
  if (!child_executor_->Next(&child_tuple, &rid_t)) {
    if (not_first_call_) {
      return false;
    }
    not_first_call_ = true;
    Schema scm{std::vector{Column{"v1", TypeId::INTEGER}}};
    std::vector<Value> values;
    values.push_back(ValueFactory::GetIntegerValue(s));
    *tuple = Tuple(std::move(values), &scm);
    return true;
  }
  TupleMeta meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
  not_first_call_ = true;
  do {
    rid_t = table_info_->table_->InsertTuple(meta, child_tuple).value();
    // std::cout << "  insert_child_tuple_rid: " << rid_t.ToString();
    // std::cout << "  insert_child_tuple: " << child_tuple.ToString(&table_info_->schema_) << '\n';
    int len = index_info_.size();
    if (0 != len) {
      int i = 0;
      while (i < len) {
        index_info_[i]->index_->InsertEntry(child_tuple.KeyFromTuple(table_info_->schema_, index_info_[i]->key_schema_,
                                                                     index_info_[i]->index_->GetKeyAttrs()),
                                            rid_t, exec_ctx_->GetTransaction());
        ++i;
      }
    }
    ++s;
  } while (child_executor_->Next(&child_tuple, &rid_t));
  Schema scm{std::vector{Column{"v1", TypeId::INTEGER}}};
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(s));
  *tuple = Tuple(std::move(values), &scm);
  return true;
}

}  // namespace bustub
