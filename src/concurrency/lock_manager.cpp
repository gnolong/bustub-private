//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/enums/statement_type.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {

  table_lock_map_latch_.lock();
  if(table_lock_map_.count(oid) == 0){
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  table_lock_map_latch_.unlock();

  auto ptr = table_lock_map_[oid];

  txn->LockTxn();
  auto txnid = txn->GetTransactionId();
  switch(txn->GetIsolationLevel()){
    case IsolationLevel::READ_UNCOMMITTED:

      if(txn->GetState() == TransactionState::GROWING){
        if(lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE){
  std::unique_lock<std::mutex> ulq(ptr->latch_);
          if(ptr->CheckUpgrade(txn, lock_mode, oid) == 0){
            txn->UnlockTxn();
            return true;
          }
          auto *lrq = new LockRequest(txnid, lock_mode, oid);
          ptr->request_queue_.emplace_back(lrq);
          while(!ptr->GrantLock(txn, lrq)){
            ptr->cv_.wait(ulq);
          }
          txn->UnlockTxn();
          return true;
        }
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txnid, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      else if(txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_SHRINKING);
      }
      else{
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_ANOTHER_PHASE);
      }
      break;

    case IsolationLevel::READ_COMMITTED:

      if(txn->GetState() == TransactionState::GROWING){
  std::unique_lock<std::mutex> ulq(ptr->latch_);
        if(ptr->CheckUpgrade(txn, lock_mode, oid) == 0){
          txn->UnlockTxn();
          return true;
        }
        auto *lrq = new LockRequest(txnid, lock_mode, oid);
        ptr->request_queue_.emplace_back(lrq);
        while(!ptr->GrantLock(txn, lrq)){
          ptr->cv_.wait(ulq);
        }
        txn->UnlockTxn();
        return true;
      }
      else if(txn->GetState() == TransactionState::SHRINKING){
        if(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED){
  std::unique_lock<std::mutex> ulq(ptr->latch_);
            if(ptr->CheckUpgrade(txn, lock_mode, oid) == 0){
              txn->UnlockTxn();
              return true;
            }
            auto *lrq = new LockRequest(txnid, lock_mode, oid);
            ptr->request_queue_.emplace_back(lrq);
            while(!ptr->GrantLock(txn, lrq)){
              ptr->cv_.wait(ulq);
            }
            txn->UnlockTxn();
            return true;
        }
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_SHRINKING);
      }
      else{
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_ANOTHER_PHASE);
      }
      break;

    case IsolationLevel::REPEATABLE_READ:
      if(txn->GetState() == TransactionState::GROWING){
  std::unique_lock<std::mutex> ulq(ptr->latch_);
        if(ptr->CheckUpgrade(txn, lock_mode, oid) == 0){
          txn->UnlockTxn();
          return true;
        }
        auto *lrq = new LockRequest(txnid, lock_mode, oid);
        ptr->request_queue_.emplace_back(lrq);
        while(!ptr->GrantLock(txn, lrq)){
          ptr->cv_.wait(ulq);
        }
        txn->UnlockTxn();
        return true;
      }
      else if(txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_SHRINKING);
      }
      else{
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_ANOTHER_PHASE);
      }
      break;
  }
  txn->UnlockTxn();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if(table_lock_map_.count(oid) == 0){
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  table_lock_map_latch_.unlock();

  auto ptr = table_lock_map_[oid];

  std::unique_lock<std::mutex> ulq(ptr->latch_);
  txn->LockTxn();
  auto txnid = txn->GetTransactionId();
  
  auto & rq = ptr->request_queue_;
  for(auto itr = rq.begin(); itr != rq.end(); ++itr){
    if((*itr)->granted_ && (*itr)->txn_id_ == txnid){
      if((*(txn->GetSharedRowLockSet()))[oid].empty() && (*(txn->GetExclusiveRowLockSet()))[oid].empty()){
        if(txn->GetState() == TransactionState::GROWING){
          if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
            if((*itr)->lock_mode_ == LockMode::SHARED ||(*itr)->lock_mode_ == LockMode::EXCLUSIVE){
              txn->SetState(TransactionState::SHRINKING);
            }
          }
          else{
            if((*itr)->lock_mode_ == LockMode::EXCLUSIVE){
              txn->SetState(TransactionState::SHRINKING);
            }
          }
        }
        switch ((*itr)->lock_mode_) {
          case LockMode::INTENTION_SHARED:
            txn->GetIntentionSharedTableLockSet()->erase(oid);
            break;
          case LockMode::INTENTION_EXCLUSIVE:
            txn->GetIntentionExclusiveTableLockSet()->erase(oid);
            break;
          case LockMode::SHARED:
            txn->GetSharedTableLockSet()->erase(oid);
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
            break;
          case LockMode::EXCLUSIVE:
            txn->GetExclusiveTableLockSet()->erase(oid);
            break;
        }
        delete *itr;
        rq.erase(itr);
        ptr->cv_.notify_all();
        txn->UnlockTxn();
        return true;
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txnid, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS); 
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if(row_lock_map_.count(rid) == 0){
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  row_lock_map_latch_.unlock();
  auto ptr = row_lock_map_[rid];

  table_lock_map_latch_.lock();
  if(table_lock_map_.count(oid) == 0){
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  table_lock_map_latch_.unlock();
  auto ptr_table = table_lock_map_[oid];

  txn->LockTxn();
  auto txnid = txn->GetTransactionId();

  if(lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if(txn->GetState() == TransactionState::GROWING || (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
                                                        txn->GetState() == TransactionState::SHRINKING &&
                                                        lock_mode == LockMode::SHARED)){
    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::SHARED){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txnid,AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    LockMode mode_t = LockMode::INTENTION_EXCLUSIVE;
    if(lock_mode == LockMode::SHARED){
      mode_t = LockMode::INTENTION_SHARED;
    }
    std::unique_lock<std::mutex> ulq_table(ptr_table->latch_);
    if(ptr_table->CheckUpgrade(txn, mode_t, oid) != 0){
      auto *lrq = new LockRequest(txnid, mode_t, oid);
      ptr_table->request_queue_.emplace_back(lrq);
      while(!ptr_table->GrantLock(txn, lrq)){
        ptr_table->cv_.wait(ulq_table);
      }
    }
    ulq_table.unlock();
    std::unique_lock<std::mutex> ulq(ptr->latch_);
    if(ptr->CheckUpgrade(txn, lock_mode, oid, rid) == 0){
      txn->UnlockTxn();
      return true;
    }
    auto *lrq = new LockRequest(txnid, lock_mode, oid, rid);
    ptr->request_queue_.emplace_back(lrq);
    while(!ptr->GrantLock(txn, lrq, 0)){
      ptr->cv_.wait(ulq);
    }
    txn->UnlockTxn();
    return true;
  }
  if(txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txnid, AbortReason::LOCK_ON_SHRINKING);
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txnid, AbortReason::LOCK_ON_ANOTHER_PHASE);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if(row_lock_map_.count(rid) == 0){
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  row_lock_map_latch_.unlock();
  auto ptr_row = row_lock_map_[rid];

  table_lock_map_latch_.lock();
  if(table_lock_map_.count(oid) == 0){
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  table_lock_map_latch_.unlock();
  auto ptr = table_lock_map_[oid];

  std::unique_lock<std::mutex> ulq_row(ptr_row->latch_);

  txn->LockTxn();
  auto txnid = txn->GetTransactionId();

  auto & rq_row = ptr_row->request_queue_;
  for(auto itr = rq_row.begin(); itr != rq_row.end(); ++itr){
    if((*itr)->granted_ && (*itr)->txn_id_ == txnid){
      if(txn->GetState() == TransactionState::GROWING){
        if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
          if((*itr)->lock_mode_ == LockMode::SHARED ||(*itr)->lock_mode_ == LockMode::EXCLUSIVE){
            txn->SetState(TransactionState::SHRINKING);
          }
        }
        else{
          if((*itr)->lock_mode_ == LockMode::EXCLUSIVE){
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      switch ((*itr)->lock_mode_) {
        case LockMode::SHARED:
          (*(txn->GetSharedRowLockSet()))[oid].erase(rid);
          break;
        case LockMode::EXCLUSIVE:
          (*(txn->GetExclusiveRowLockSet()))[oid].erase(rid);
          break;
        case LockMode::INTENTION_SHARED:
        case LockMode::INTENTION_EXCLUSIVE:
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
          break;
      }

      auto mode_t = (*itr)->lock_mode_;
      delete *itr;
      rq_row.erase(itr);
      ptr_row->cv_.notify_all();
      ulq_row.unlock();
      
      if((*(txn->GetSharedRowLockSet()))[oid].empty() && (*(txn->GetExclusiveRowLockSet()))[oid].empty()){
        if(mode_t == LockMode::SHARED){
          mode_t = LockMode::INTENTION_SHARED;
        }
        mode_t = LockMode::INTENTION_EXCLUSIVE;
        std::unique_lock<std::mutex> ulq(ptr->latch_);
        auto & rq = ptr->request_queue_;
        for(auto itr_table = rq.begin(); itr_table != rq.end(); ++itr_table){
          if((*itr_table)->granted_ && (*itr_table)->txn_id_ == txnid && (*itr)->lock_mode_ == mode_t){

            switch (mode_t) {
              case LockMode::INTENTION_SHARED:{
                txn->GetIntentionSharedTableLockSet()->erase(oid);
              }
                break;
              case LockMode::INTENTION_EXCLUSIVE:{
                txn->GetIntentionExclusiveTableLockSet()->erase(oid);
              }
                break;
              case LockMode::SHARED:
              case LockMode::EXCLUSIVE:
              case LockMode::SHARED_INTENTION_EXCLUSIVE:
                break;
            }

            delete *itr_table;
            rq.erase(itr);
            ptr->cv_.notify_all();
            txn->UnlockTxn();
            return true;

          }
        }
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txnid, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS); 
      }
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
