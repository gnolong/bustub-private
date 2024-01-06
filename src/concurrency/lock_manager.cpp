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
#include <algorithm>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/enums/statement_type.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
using std::vector;

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  table_lock_map_latch_.unlock();

  auto ptr = table_lock_map_[oid];

  txn->LockTxn();
  auto txnid = txn->GetTransactionId();
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:

      if (txn->GetState() == TransactionState::GROWING) {
        if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
          std::unique_lock<std::mutex> ulq(ptr->latch_);
          if (ptr->CheckUpgrade(txn, lock_mode, oid) == 0) {
            txn->UnlockTxn();
            return true;
          }
          auto *lrq = new LockRequest(txnid, lock_mode, oid);
          ptr->request_queue_.emplace_back(lrq);
          while (!ptr->GrantLock(txn, lrq)) {
            ptr->cv_.wait(ulq);
            if (txn->GetState() == TransactionState::ABORTED) {
              delete lrq;
              ptr->request_queue_.remove(lrq);
              txn->UnlockTxn();
              return false;
            }
          }
          txn->UnlockTxn();
          return true;
        }
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      } else if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_SHRINKING);
      } else {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_ANOTHER_PHASE);
      }
      break;

    case IsolationLevel::READ_COMMITTED:

      if (txn->GetState() == TransactionState::GROWING) {
        std::unique_lock<std::mutex> ulq(ptr->latch_);
        if (ptr->CheckUpgrade(txn, lock_mode, oid) == 0) {
          txn->UnlockTxn();
          return true;
        }
        auto *lrq = new LockRequest(txnid, lock_mode, oid);
        ptr->request_queue_.emplace_back(lrq);
        while (!ptr->GrantLock(txn, lrq)) {
          ptr->cv_.wait(ulq);
          if (txn->GetState() == TransactionState::ABORTED) {
            delete lrq;
            ptr->request_queue_.remove(lrq);
            txn->UnlockTxn();
            return false;
          }
        }
        txn->UnlockTxn();
        return true;
      } else if (txn->GetState() == TransactionState::SHRINKING) {
        if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED) {
          std::unique_lock<std::mutex> ulq(ptr->latch_);
          if (ptr->CheckUpgrade(txn, lock_mode, oid) == 0) {
            txn->UnlockTxn();
            return true;
          }
          auto *lrq = new LockRequest(txnid, lock_mode, oid);
          ptr->request_queue_.emplace_back(lrq);
          while (!ptr->GrantLock(txn, lrq)) {
            ptr->cv_.wait(ulq);
            if (txn->GetState() == TransactionState::ABORTED) {
              delete lrq;
              ptr->request_queue_.remove(lrq);
              txn->UnlockTxn();
              return false;
            }
          }
          txn->UnlockTxn();
          return true;
        }
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_SHRINKING);
      } else {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_ANOTHER_PHASE);
      }
      break;

    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::GROWING) {
        std::unique_lock<std::mutex> ulq(ptr->latch_);
        if (ptr->CheckUpgrade(txn, lock_mode, oid) == 0) {
          txn->UnlockTxn();
          return true;
        }
        auto *lrq = new LockRequest(txnid, lock_mode, oid);
        ptr->request_queue_.emplace_back(lrq);
        while (!ptr->GrantLock(txn, lrq)) {
          ptr->cv_.wait(ulq);
          if (txn->GetState() == TransactionState::ABORTED) {
            delete lrq;
            ptr->request_queue_.remove(lrq);
            txn->UnlockTxn();
            return false;
          }
        }
        txn->UnlockTxn();
        return true;
      } else if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_SHRINKING);
      } else {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::LOCK_ON_ANOTHER_PHASE);
      }
      break;
  }
  txn->UnlockTxn();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  table_lock_map_latch_.unlock();

  auto ptr = table_lock_map_[oid];

  std::unique_lock<std::mutex> ulq(ptr->latch_);
  txn->LockTxn();
  auto txnid = txn->GetTransactionId();

  auto &rq = ptr->request_queue_;
  for (auto itr = rq.begin(); itr != rq.end(); ++itr) {
    if ((*itr)->granted_ && (*itr)->txn_id_ == txnid) {
      if ((*(txn->GetSharedRowLockSet()))[oid].empty() && (*(txn->GetExclusiveRowLockSet()))[oid].empty()) {
        if (txn->GetState() == TransactionState::GROWING) {
          if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
            if ((*itr)->lock_mode_ == LockMode::SHARED || (*itr)->lock_mode_ == LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::SHRINKING);
            }
          } else {
            if ((*itr)->lock_mode_ == LockMode::EXCLUSIVE) {
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
      txn->UnlockTxn();
      throw TransactionAbortException(txnid, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
  }
  txn->SetState(TransactionState::ABORTED);
  txn->UnlockTxn();
  throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  row_lock_map_latch_.unlock();
  auto ptr = row_lock_map_[rid];

  txn->LockTxn();
  auto txnid = txn->GetTransactionId();

  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetState() == TransactionState::GROWING ||
      (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->GetState() == TransactionState::SHRINKING &&
       lock_mode == LockMode::SHARED)) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txnid, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (lock_mode == LockMode::SHARED) {
      if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedLocked(oid) &&
          !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
          !txn->IsTableExclusiveLocked(oid)) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
    } else {
      if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
          !txn->IsTableExclusiveLocked(oid)) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txnid, AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
    }
    std::unique_lock<std::mutex> ulq(ptr->latch_);
    if (ptr->CheckUpgrade(txn, lock_mode, oid, rid) == 0) {
      txn->UnlockTxn();
      return true;
    }
    auto *lrq = new LockRequest(txnid, lock_mode, oid, rid);
    ptr->request_queue_.emplace_back(lrq);
    while (!ptr->GrantLock(txn, lrq, 0)) {
      ptr->cv_.wait(ulq);
      if (txn->GetState() == TransactionState::ABORTED) {
        delete lrq;
        // std::cout << "before remove qu.size()" << ptr->request_queue_.size() << "\n";
        ptr->request_queue_.remove(lrq);
        // std::cout << "after remove qu.size()" << ptr->request_queue_.size() << "\n";
        txn->UnlockTxn();
        return false;
      }
    }
    txn->UnlockTxn();
    return true;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txnid, AbortReason::LOCK_ON_SHRINKING);
  }
  txn->SetState(TransactionState::ABORTED);
  txn->UnlockTxn();
  throw TransactionAbortException(txnid, AbortReason::LOCK_ON_ANOTHER_PHASE);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  row_lock_map_latch_.unlock();
  auto ptr_row = row_lock_map_[rid];

  std::unique_lock<std::mutex> ulq_row(ptr_row->latch_);

  txn->LockTxn();
  auto txnid = txn->GetTransactionId();

  auto &rq_row = ptr_row->request_queue_;
  for (auto itr = rq_row.begin(); itr != rq_row.end(); ++itr) {
    if ((*itr)->granted_ && (*itr)->txn_id_ == txnid) {
      if (force) {
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
            txn->UnlockTxn();
            throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
        }
        delete *itr;
        rq_row.erase(itr);
        ptr_row->cv_.notify_all();
        txn->UnlockTxn();
        return true;
      }
      if (txn->GetState() == TransactionState::GROWING) {
        if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
          if ((*itr)->lock_mode_ == LockMode::SHARED || (*itr)->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        } else {
          if ((*itr)->lock_mode_ == LockMode::EXCLUSIVE) {
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
          txn->UnlockTxn();
          throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      }

      delete *itr;
      rq_row.erase(itr);
      ptr_row->cv_.notify_all();
      txn->UnlockTxn();
      return true;
    }
  }
  txn->SetState(TransactionState::ABORTED);
  txn->UnlockTxn();
  throw TransactionAbortException(txnid, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> uql(waits_for_latch_);
  auto &vec = waits_for_[t1];
  (void)waits_for_[t2];
  for (auto &ve : vec) {
    if (ve == t2) {
      return;
    }
  }
  vec.emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> uql(waits_for_latch_);
  auto &vec = waits_for_[t1];
  auto itr = vec.begin();
  for (; itr != vec.end(); ++itr) {
    if (*itr == t2) {
      break;
    }
  }
  if (itr != vec.end()) {
    vec.erase(itr);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_map<txn_id_t, int> mp;
  std::vector<txn_id_t> vec;
  vec.reserve(waits_for_.size());
  for (auto &wait : waits_for_) {
    vec.push_back(wait.first);
    mp[wait.first] = 0;
    std::sort(wait.second.begin(), wait.second.end());
  }
  std::sort(vec.begin(), vec.end());
  txn_id_t id_t = INVALID_TXN_ID;
  for (auto &ve :vec) {
    if (mp[ve] == 0) {
      DfsTxn(mp, ve, &id_t, ve);
    }
  }
  if (id_t == INVALID_TXN_ID) {
    return false;
  }
  *txn_id = id_t;
  return true;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &wait : waits_for_) {
    for (auto &id : wait.second) {
      edges.emplace_back(wait.first, id);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> ul_wait(waits_for_latch_);
      std::unique_lock<std::mutex> ul_table(table_lock_map_latch_);
      std::unique_lock<std::mutex> ul_row(row_lock_map_latch_);
      // std::this_thread::sleep_for(cycle_detection_interval);
      std::vector<txn_id_t> granted;
      std::vector<txn_id_t> ungranted;
      std::unordered_map<txn_id_t, std::vector<LockRequestQueue*>> cv;
      // std::cout << "table" << '\n';
      for(auto &table : table_lock_map_){
        std::unique_lock<std::mutex> ul(table.second->latch_);
        auto & qu = table.second->request_queue_;
        for(auto &lrq : qu){
          // std::cout << "tableid " << table.first << " txn_id " << lrq->txn_id_ << " granted " << lrq->granted_<< '\n';
          if(lrq->granted_){
            granted.push_back(lrq->txn_id_);
          }
          else{
            ungranted.push_back(lrq->txn_id_);
            cv[lrq->txn_id_].push_back(table.second.get());
          }
        }
        for(auto &id_un : ungranted){
          for(auto &id_grant : granted){
            AddEdge(id_un, id_grant);
          }
        }
        granted.clear();
        ungranted.clear();
      }
      // std::cout << "row" << '\n';
      for(auto &table : row_lock_map_){
        std::unique_lock<std::mutex> ul(table.second->latch_);
        auto & qu = table.second->request_queue_;
        // std::cout << "row.size()" << qu.size() << '\n';
        for(auto &lrq : qu){
          // std::cout << "rowid " << table.first << " txn_id " << lrq->txn_id_ << " granted " << lrq->granted_<< '\n';
          if(lrq->granted_){
            granted.push_back(lrq->txn_id_);
          }
          else{
            ungranted.push_back(lrq->txn_id_);
            cv[lrq->txn_id_].push_back(table.second.get());
          }
        }
        for(auto &id_un : ungranted){
          for(auto &id_grant : granted){
            AddEdge(id_un, id_grant);
          }
        }
        granted.clear();
        ungranted.clear();
      }
      txn_id_t txn_id = INVALID_TXN_ID;
      while(HasCycle(&txn_id)){
        TransactionManager::GetTransaction(txn_id)->SetState(TransactionState::ABORTED);
        for(auto &id : waits_for_[txn_id]){
          RemoveEdge(txn_id, id);
        }
        for(auto &c : cv[txn_id]){
          // std::unique_lock<std::mutex> ul(c->latch_);
          c->cv_.notify_all();
        }
        // std::cout << "fix one cycle" << '\n';
      }
      waits_for_.clear();
    }
  }
}
void LockManager::DfsTxn(std::unordered_map<txn_id_t, int> &mp, txn_id_t id_p, txn_id_t *id_t, txn_id_t id_young) {
  if (*id_t != INVALID_TXN_ID) {
    return;
  }
  auto &vec = waits_for_[id_p];
  for (auto &vc : vec) {
    if (mp[vc] == 2) {
      *id_t = id_young;
      return;
    }
    if (mp[vc] == 0) {
      mp[vc] = 2;
      DfsTxn(mp, vc, id_t, std::max(id_young, vc));
      mp[vc] = 1;
    }
  }
}
auto LockManager::LockRequestQueue::GrantLock(Transaction *txn, LockRequest *lock_req) -> bool {
  if (upgrading_ != INVALID_TXN_ID && upgrading_ != lock_req->txn_id_) {
    return false;
  }
  for (auto &lrq : request_queue_) {
    if (lrq->granted_ &&
        !COMPATIBLE_MATRIX[static_cast<int>(lrq->lock_mode_)][static_cast<int>(lock_req->lock_mode_)]) {
      return false;
    }
  }
  if (upgrading_ == lock_req->txn_id_) {
    upgrading_ = INVALID_TXN_ID;
  }
  switch (lock_req->lock_mode_) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->emplace(lock_req->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->emplace(lock_req->oid_);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->emplace(lock_req->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->emplace(lock_req->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->emplace(lock_req->oid_);
      break;
  }
  lock_req->granted_ = true;
  return true;
}
auto LockManager::LockRequestQueue::GrantLock(Transaction *txn, LockRequest *lock_req, int a) -> bool {
  if (upgrading_ != INVALID_TXN_ID && upgrading_ != lock_req->txn_id_) {
    return false;
  }
  for (auto &lrq : request_queue_) {
    if (lrq->granted_ &&
        !COMPATIBLE_MATRIX[static_cast<int>(lrq->lock_mode_)][static_cast<int>(lock_req->lock_mode_)]) {
      return false;
    }
  }
  if (upgrading_ == lock_req->txn_id_) {
    upgrading_ = INVALID_TXN_ID;
  }
  switch (lock_req->lock_mode_) {
    case LockMode::SHARED:
      (*(txn->GetSharedRowLockSet()))[lock_req->oid_].emplace(lock_req->rid_);
      break;
    case LockMode::EXCLUSIVE:
      (*(txn->GetExclusiveRowLockSet()))[lock_req->oid_].emplace(lock_req->rid_);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
  lock_req->granted_ = true;
  return true;
}
auto LockManager::LockRequestQueue::CheckUpgrade(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> int {
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      if (txn->IsTableIntentionSharedLocked(oid)) {
        return 0;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (txn->IsTableIntentionExclusiveLocked(oid)) {
        return 0;
      }
      break;
    case LockMode::SHARED:
      if (txn->IsTableSharedLocked(oid)) {
        return 0;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        return 0;
      }
      break;
    case LockMode::EXCLUSIVE:
      if (txn->IsTableExclusiveLocked(oid)) {
        return 0;
      }
      break;
  }
  int j = static_cast<int>(lock_mode);
  auto txn_id = txn->GetTransactionId();
  for (auto lrq = request_queue_.begin(); lrq != request_queue_.end(); ++lrq) {
    if ((*lrq)->granted_ && (*lrq)->txn_id_ == txn_id) {
      if (UPGRADE_MATRIX[static_cast<int>((*lrq)->lock_mode_)][j]) {
        if (upgrading_ != INVALID_TXN_ID) {
          txn->SetState(TransactionState::ABORTED);
          txn->UnlockTxn();
          throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
        }
        upgrading_ = txn_id;
        switch ((*lrq)->lock_mode_) {
          case LockMode::INTENTION_SHARED:
            txn->GetIntentionSharedTableLockSet()->erase((*lrq)->oid_);
            break;
          case LockMode::INTENTION_EXCLUSIVE:
            txn->GetIntentionExclusiveTableLockSet()->erase((*lrq)->oid_);
            break;
          case LockMode::SHARED:
            txn->GetSharedTableLockSet()->erase((*lrq)->oid_);
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            txn->GetSharedIntentionExclusiveTableLockSet()->erase((*lrq)->oid_);
            break;
          case LockMode::EXCLUSIVE:
            txn->GetExclusiveTableLockSet()->erase((*lrq)->oid_);
            break;
        }
        delete *lrq;
        request_queue_.erase(lrq);
        return 1;
      }
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }
  return 2;
}
auto LockManager::LockRequestQueue::CheckUpgrade(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                                 const RID &rid) -> int {
  switch (lock_mode) {
    case LockMode::SHARED:
      if (txn->IsRowSharedLocked(oid, rid)) {
        return 0;
      }
      break;
    case LockMode::EXCLUSIVE:
      if (txn->IsRowExclusiveLocked(oid, rid)) {
        return 0;
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
  int j = static_cast<int>(lock_mode);
  auto txn_id = txn->GetTransactionId();
  for (auto lrq = request_queue_.begin(); lrq != request_queue_.end(); ++lrq) {
    if ((*lrq)->granted_ && (*lrq)->txn_id_ == txn_id) {
      if (UPGRADE_MATRIX[static_cast<int>((*lrq)->lock_mode_)][j]) {
        if (upgrading_ != INVALID_TXN_ID) {
          txn->SetState(TransactionState::ABORTED);
          txn->UnlockTxn();
          throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
        }
        upgrading_ = txn_id;
        switch ((*lrq)->lock_mode_) {
          case LockMode::SHARED:
            (*(txn->GetSharedRowLockSet()))[(*lrq)->oid_].erase((*lrq)->rid_);
            break;
          case LockMode::EXCLUSIVE:
            (*(txn->GetExclusiveRowLockSet()))[(*lrq)->oid_].erase((*lrq)->rid_);
            break;
          case LockMode::INTENTION_SHARED:
          case LockMode::INTENTION_EXCLUSIVE:
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            break;
        }
        delete *lrq;
        request_queue_.erase(lrq);
        return 1;
      }
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }
  return 2;
}

}  // namespace bustub
