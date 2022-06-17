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
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, bool granted)
        : txn_id_(txn_id), lock_mode_(lock_mode), granted_(granted) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    // 一开始实现时granted没用到（一个txn应该是串行的)
    // 但是当要进行死锁预防时，需要看到哪些txn在等待分配锁，又重新加上了。后来发现实际也没啥用
    bool granted_;

    // 在wound-wait死锁预防中，可能abort掉已经拿到锁的txn，这时他不会被唤醒，因此需要标记从而在申请锁时不管它
    // bool is_aborted_;
  };

  class LockRequestQueue {
   public:
    // 按照到来的顺序从前往后加
    std::list<LockRequest> request_queue_;
    // for notifying blocked transactions on this rid
    std::condition_variable cv_;
    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;
    // 本来是想这样细粒度上锁的,但不知道哪里出了问题,先大锁用着（后面又超时了，改成小锁）
    // 对每个tuple的queue上锁
    std::mutex query_latch_;
    // 加锁互斥量
    //    std::mutex mutex_;
    //    std::unique_lock<std::mutex> lck_;
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() = default;

  ~LockManager() = default;

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  bool GetMyLatch() { return latch_.try_lock(); }
  void UnLockLatch() { latch_.unlock(); }

 private:
  std::mutex latch_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;

  // 预先检查是否能上锁
  bool PreCheckLock(Transaction *txn, const RID &rid) {
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    return true;
  }

  // 构造空的等待队列
  void ConstructQueue(Transaction *txn, const RID &rid) {
    if (lock_table_.find(rid) == lock_table_.end()) {
      // mutex和condition_variable不能被复制或移动,只能采取在map中原地构造的方式将其加入，即使用emplace()，并且需要配合pair’s
      // piecewise constructor。https://www.cnblogs.com/guxuanqing/p/11396511.html
      lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    }
  }

  // Abort所有新的Txn，wound-wait算法
  void AbortNewTxn(Transaction *txn, const RID &rid, bool IsReadLock, LockRequestQueue *request_queue);

  // 检查是否被Abort导致唤醒
  bool CheckAbortedL(Transaction *txn, const RID &rid) {
    // 被abort唤醒返回false
    return txn->GetState() == TransactionState::ABORTED;
    //    if (txn->GetState() == TransactionState::ABORTED) {
    //      return true;
    // 因为此时没有加入txn的LockSet，导致后续不会自动放锁，这里需要放
    //      lock_table_[rid].query_latch_.lock();
    //      for (auto my_request = lock_table_[rid].request_queue_.begin();
    //           my_request != lock_table_[rid].request_queue_.end(); ++my_request) {
    //        if (my_request->txn_id_ == txn->GetTransactionId()) {
    //          lock_table_[rid].request_queue_.erase(my_request);
    //          break;
    //        }
    //      }
    //      // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    //      return true;
    //    }
    //    return false;
  }

  // 检查是否能上读锁
  bool CheckSLockL(Transaction *txn, const RID &rid, std::list<LockRequest>::iterator *my_request,
                   LockRequestQueue *request_queue) {
    // 获取加入队列时的位置
    for (std::list<LockRequest>::iterator item = request_queue->request_queue_.begin();
         item != request_queue->request_queue_.end(); ++item) {
      if (item->txn_id_ == txn->GetTransactionId()) {
        *my_request = item;
        break;
      }
      if (item->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
      // 不用这个判断速度可能快些
      // if (!item->granted_) {
      //    return false;
      // }
    }
    return true;
  }

  // 检查是否能上写锁
  bool CheckXLockL(Transaction *txn, const RID &rid, std::list<LockRequest>::iterator *my_request,
                   LockRequestQueue *request_queue) {
    // 在最前面才加写锁
    if (request_queue->request_queue_.front().txn_id_ == txn->GetTransactionId()) {
      *my_request = request_queue->request_queue_.begin();
      return true;
    }
    return false;
  }
};

}  // namespace bustub
