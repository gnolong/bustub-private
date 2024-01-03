//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode {INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED, SHARED_INTENTION_EXCLUSIVE, EXCLUSIVE};
  static constexpr bool COMPATIBLE_MATRIX[5][5] = {
      {true, true, true, true, false},
      {true, true, false, false, false},
      {true, false, true, false, false},
      {true, false, false, false, false},
      {false, false, false, false, false}
  };
  
  static constexpr bool UPGRADE_MATRIX[5][5] = {
      {false, true, true, true, true},
      {false, false, false, true, true},
      {false, false, false, true, true},
      {false, false, false, false, true},
      {false, false, false, false, false}
  };
  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<LockRequest*> request_queue_;//change ptr type to LockRequest by wl
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;

    auto GrantLock(Transaction *txn, LockRequest* lock_req)-> bool{
      if(upgrading_ != INVALID_TXN_ID && upgrading_ != lock_req->txn_id_){
          return false;
      }
      for(auto &lrq : request_queue_){
        if(lrq->granted_ && !COMPATIBLE_MATRIX[static_cast<int>(lrq->lock_mode_)][static_cast<int>(lock_req->lock_mode_)]){
          return false;
        }
      }
      if(upgrading_ == lock_req->txn_id_){
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
    auto GrantLock(Transaction *txn, LockRequest* lock_req, int a)-> bool{
      if(upgrading_ != INVALID_TXN_ID && upgrading_ != lock_req->txn_id_){
          return false;
      }
      for(auto &lrq : request_queue_){
        if(lrq->granted_ && !COMPATIBLE_MATRIX[static_cast<int>(lrq->lock_mode_)][static_cast<int>(lock_req->lock_mode_)]){
          return false;
        }
      }
      if(upgrading_ == lock_req->txn_id_){
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
    auto CheckUpgrade(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> int {
      switch (lock_mode) {
        case LockMode::INTENTION_SHARED:
          if(txn->IsTableIntentionSharedLocked(oid)){
            return 0;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if(txn->IsTableIntentionExclusiveLocked(oid)){
            return 0;
          }
          break;
        case LockMode::SHARED:
          if(txn->IsTableSharedLocked(oid)){
            return 0;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if(txn->IsTableSharedIntentionExclusiveLocked(oid)){
            return 0;
          }
          break;
        case LockMode::EXCLUSIVE:
          if(txn->IsTableExclusiveLocked(oid)){
            return 0;
          }
          break;
      }
      int j = static_cast<int>(lock_mode);
      auto txn_id = txn->GetTransactionId();
      for(auto lrq = request_queue_.begin(); lrq != request_queue_.end(); ++lrq){
        if((*lrq)->granted_ && (*lrq)->txn_id_ == txn_id){
          if(UPGRADE_MATRIX[static_cast<int>((*lrq)->lock_mode_)][j]){
            if(upgrading_ != INVALID_TXN_ID){
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
          throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
         
        }
      }
      return 2;
    }
    auto CheckUpgrade(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> int {
      switch (lock_mode) {
        case LockMode::SHARED:
          if(txn->IsRowSharedLocked(oid, rid)){
            return 0;
          }
          break;
        case LockMode::EXCLUSIVE:
          if(txn->IsRowExclusiveLocked(oid, rid)){
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
      for(auto lrq = request_queue_.begin(); lrq != request_queue_.end(); ++lrq){
        if((*lrq)->granted_ && (*lrq)->txn_id_ == txn_id){
          if(UPGRADE_MATRIX[static_cast<int>((*lrq)->lock_mode_)][j]){
            if(upgrading_ != INVALID_TXN_ID){
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
          throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
      return 2;
    }
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @param force unlock the tuple regardless of isolation level, not changing the transaction state
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force = false) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

 private:
  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;
};

}  // namespace bustub
