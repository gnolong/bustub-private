//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashjoinKey {
  std::vector<Value> values_;

  /**
   * Compares two Join keys for equality.
   * @param other the other Join key to be compared with
   * @return `true` if both Join keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const HashjoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.values_.size(); i++) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** HashjoinValue represents a value for each of the running Joins */
struct HashjoinValue {
  /** The Join values */
  std::vector<std::vector<Value>> valuess_;
};

}
namespace std {

/** Implements std::hash on HashjoinKey */
template <>
struct hash<bustub::HashjoinKey> {
  auto operator()(const bustub::HashjoinKey &key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : key.values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
namespace bustub {


/**
 * A simplified hash table that has all the necessary functionality for Joins.
 */
class SimpleJoinHashTable {
 public:
  SimpleJoinHashTable() = default;
  /**
   * Inserts a value into the hash table and then combines it with the current Join.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void Insert(const HashjoinKey &key, const std::vector<Value> &values) {
    ht_[key].valuess_.push_back(values);
  }
  auto Count(const HashjoinKey &key) -> int{
    return ht_.count(key);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the Join hash table */
  class Iterator {
   public:
    /** Creates an iterator for the Join map. */
    explicit Iterator(std::unordered_map<HashjoinKey, HashjoinValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const HashjoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const HashjoinValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Joins map */
    std::unordered_map<HashjoinKey, HashjoinValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

  auto operator[](const HashjoinKey& key) -> HashjoinValue&{
    return ht_[key];
  }
 private:
  /** The hash table is just a map from Join keys to Join values */
  std::unordered_map<HashjoinKey, HashjoinValue> ht_{};
};


/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an JoinKey */
  enum JType {left, right};

  auto MakeJoinKey(const Tuple *tuple, const JType type) -> HashjoinKey {
    std::vector<Value> keys;
    if(type == JType::left){
      for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
        keys.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
      }
    }
    else if(type == JType::right){
      for (const auto &expr : plan_->RightJoinKeyExpressions()) {
        keys.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
      }
    }
    return {keys};
  }

  /** @return The tuple as an JoinValue */
  auto MakeJoinValue(const Tuple *tuple, const JType type) -> std::vector<Value> {
    std::vector<Value> vals;
    if(type == JType::left){
      auto schema = plan_->GetLeftPlan()->OutputSchema();
      auto len = schema.GetColumnCount();
      for(uint32_t i = 0; i < len; ++i){
        vals.emplace_back(tuple->GetValue(&schema, i));
      }
    }
    if(type == JType::right){
      auto schema = plan_->GetRightPlan()->OutputSchema();
      auto len = schema.GetColumnCount();
      for(uint32_t i = 0; i < len; ++i){
        vals.emplace_back(tuple->GetValue(&schema, i));
      }
    }
    return vals;
  }
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;

  std::unique_ptr<AbstractExecutor> right_child_;

  SimpleJoinHashTable aht_{};

  SimpleJoinHashTable::Iterator aht_itr_;

  // SimpleJoinHashTable rt_ht_;

  // SimpleJoinHashTable::Iterator rt_itr_;

  std::vector<std::vector<Value>> valuess_{};
  
  std::vector<std::vector<Value>>::iterator itr_;

  bool not_first_call_{false};
};

}  // namespace bustub
