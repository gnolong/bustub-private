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
              (void)std::remove(ptr->request_queue_.begin(), ptr->request_queue_.end(), lrq);
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
            (void)std::remove(ptr->request_queue_.begin(), ptr->request_queue_.end(), lrq);
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
              (void)std::remove(ptr->request_queue_.begin(), ptr->request_queue_.end(), lrq);
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
            (void)std::remove(ptr->request_queue_.begin(), ptr->request_queue_.end(), lrq);
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
        (void)std::remove(ptr->request_queue_.begin(), ptr->request_queue_.end(), lrq);
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
  // auto vec = waits_for_[t1];
  // for(add)
}

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
