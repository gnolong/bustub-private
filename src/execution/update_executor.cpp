//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //   std::cout << "throught update_executor" << '\n';
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
    auto pair = table_info_->table_->GetTuple(rid_t);
    //   std::cout << "  update_child_tuple_rid: " << rid_t.ToString();
    //   std::cout << "  update_child_tuple: " << child_tuple.ToString(&table_info_->schema_) << '\n';
    int len = index_info_.size();
    if (0 != len) {
      int i = 0;
      while (i < len) {
        index_info_[i]->index_->DeleteEntry(child_tuple.KeyFromTuple(table_info_->schema_, index_info_[i]->key_schema_,
                                                                     index_info_[i]->index_->GetKeyAttrs()),
                                            rid_t, exec_ctx_->GetTransaction());
        ++i;
      }
    }
    pair.first.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(pair.first, rid_t);

    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &exp : plan_->target_expressions_) {
      std::cout << "    Update Expressions: " << exp->ToString() << '\n';
      auto value = exp->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
      // use child's schema
      std::cout << "    update_value: " << value.ToString() << '\n';
      values.push_back(std::move(value));
    }
    Tuple new_tuple{values, &child_executor_->GetOutputSchema()};
    rid_t = table_info_->table_->InsertTuple(meta, new_tuple).value();
    if (0 != len) {
      int i = 0;
      while (i < len) {
        index_info_[i]->index_->InsertEntry(new_tuple.KeyFromTuple(table_info_->schema_, index_info_[i]->key_schema_,
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
