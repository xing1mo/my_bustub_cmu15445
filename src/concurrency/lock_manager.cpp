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
#include "concurrency/transaction_manager.h"

namespace bustub {

void LockManager::AbortNewTxn(Transaction *txn, const RID &rid, bool IsReadLock) {
  // 对于上读锁，所有txn_id大的写锁都要abort
  // 上写锁则abort所有txn_id大的
  std::vector<std::list<LockRequest>::iterator> remove_set;
  bool has_abort = false;
  for (auto lock_request = lock_table_[rid].request_queue_.begin();
       lock_request != lock_table_[rid].request_queue_.end(); ++lock_request) {
    if (lock_request->txn_id_ > txn->GetTransactionId() &&
        (!IsReadLock || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
      TransactionManager::GetTransaction(lock_request->txn_id_)->SetState(TransactionState::ABORTED);
      remove_set.push_back(lock_request);
      has_abort = true;
    }
  }
  // 就地删除所有需要abort的request
  for (auto lock_request : remove_set) {
    TransactionManager::GetTransaction(lock_request->txn_id_)->GetSharedLockSet()->erase(rid);
    TransactionManager::GetTransaction(lock_request->txn_id_)->GetExclusiveLockSet()->erase(rid);
    lock_table_[rid].request_queue_.erase(lock_request);
    //    printf("[%d]--UnLock-rid[%d]-Abort\n", lock_request->txn_id_, rid.GetPageId());
  }
  if (has_abort) {
    lock_table_[rid].cv_.notify_all();
  }
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 判断异常
  if (!PreCheckLock(txn, rid)) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  // 整个加锁，避免同时构造导致覆盖掉刚新建的lock_table_
  std::unique_lock lk(latch_);
  //  printf("[%d]GTE_LATCH-rid[%d]-SLOCK1\n", txn->GetTransactionId(), rid.GetPageId());

  // 构造空的等待队列
  if (!ConstructQueue(txn, rid, true)) {
    //    printf("[%d]UN_LATCH-rid[%d]-SLOCK1\n", txn->GetTransactionId(), rid.GetPageId());
    lk.unlock();
    return true;
  }
  //  printf("[%d]--SLock-rid[%d]-Try:Wait--  ", txn->GetTransactionId(), rid.GetPageId());
  //  for (auto requst :lock_table_[rid].request_queue_) {
  //    printf("%d ",requst.txn_id_);
  //  }
  //  printf("\n");
  // !!!!!!!!注意：即使此时没上锁也要先加入set，可能后续因为其他RID导致事务被abort，这时set中要有该rid才能成功将等待队列中的锁去除
  // 事务中加入RID
  txn->GetSharedLockSet()->emplace(rid);

  // Abort所有新的Txn
  AbortNewTxn(txn, rid, true);

  std::list<LockRequest>::iterator my_request;

  // !!!注意不同的wait要声明不同的mutex
  std::mutex mu;
  std::unique_lock<std::mutex> lck(mu);
  while (!CheckSLockL(txn, rid, &my_request)) {
    // LOG_INFO("txn:[%d] ,rid:[%d]-WaitRLock", txn->GetTransactionId(), rid.GetPageId());
    // 不能上锁,则进行等待
    //    printf("[%d]UN_LATCH-rid[%d]-SLOCK2\n", txn->GetTransactionId(), rid.GetPageId());
    lk.unlock();

    lock_table_[rid].cv_.wait(lck);

    lk.lock();
    //    printf("[%d]GTE_LATCH-rid[%d]-SLOCK2\n", txn->GetTransactionId(), rid.GetPageId());
    // 检查是否被Abort导致唤醒
    if (CheckAbortedL(txn, rid)) {
      //      printf("[%d]UN_LATCH-rid[%d]-SLOCK3\n", txn->GetTransactionId(), rid.GetPageId());
      lk.unlock();
      return false;
    }
  }
  // 能够上锁
  my_request->granted_ = true;

  //  printf("[%d]UN_LATCH-rid[%d]-SLOCK4\n", txn->GetTransactionId(), rid.GetPageId());
  lk.unlock();
  //  printf("[%d]--GetSLock-rid[%d]-Success\n", txn->GetTransactionId(), rid.GetPageId());
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 判断异常
  if (!PreCheckLock(txn, rid)) {
    return false;
  }

  // 整个加锁，避免同时构造导致覆盖掉刚新建的lock_table_
  std::unique_lock lk(latch_);
  //  printf("[%d]GTE_LATCH-rid[%d]-XLOCK1\n", txn->GetTransactionId(), rid.GetPageId());
  //  printf("[%d]-rid[%d]1\n", txn->GetTransactionId(), rid.GetPageId());
  // 构造空的等待队列
  if (!ConstructQueue(txn, rid, false)) {
    //    printf("[%d]UN_LATCH-rid[%d]-XLOCK1\n", txn->GetTransactionId(), rid.GetPageId());
    lk.unlock();
    return true;
  }
  //  printf("[%d]--XLock-rid[%d]-Try:Wait--  ", txn->GetTransactionId(), rid.GetPageId());
  //  for (auto requst :lock_table_[rid].request_queue_) {
  //    printf("%d ",requst.txn_id_);
  //  }
  //  printf("\n");

  // !!!!!!!!注意：即使此时没上锁也要先加入set，可能后续因为其他RID导致事务被abort，这时set中要有该rid才能成功将等待队列中的锁去除
  // 事务中加入RID
  txn->GetExclusiveLockSet()->emplace(rid);

  // Abort所有新的Txn
  AbortNewTxn(txn, rid, false);

  std::list<LockRequest>::iterator my_request;
  std::mutex mu;
  std::unique_lock<std::mutex> lck(mu);
  while (!CheckXLockL(txn, rid, &my_request)) {
    // LOG_INFO("txn:[%d] ,rid:[%d]-WaitWLock", txn->GetTransactionId(), rid.GetPageId());
    // 不能上锁,则进行等待
    //    printf("[%d]UN_LATCH-rid[%d]-XLOCK2\n", txn->GetTransactionId(), rid.GetPageId());
    lk.unlock();

    lock_table_[rid].cv_.wait(lck);

    lk.lock();
    //    printf("[%d]GTE_LATCH-rid[%d]-XLOCK2\n", txn->GetTransactionId(), rid.GetPageId());
    // 检查是否被Abort导致唤醒
    if (CheckAbortedL(txn, rid)) {
      //      printf("[%d]UN_LATCH-rid[%d]-XLOCK3\n", txn->GetTransactionId(), rid.GetPageId());
      lk.unlock();
      //  return false;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  // 能够上锁
  my_request->granted_ = true;

  //  printf("[%d]UN_LATCH-rid[%d]-XLOCK4\n", txn->GetTransactionId(), rid.GetPageId());
  lk.unlock();
  //  printf("[%d]--GetXLock-rid[%d]-Success\n", txn->GetTransactionId(), rid.GetPageId());
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 判断异常
  if (!PreCheckLock(txn, rid)) {
    return false;
  }

  std::unique_lock lk(latch_);
  if (lock_table_.find(rid) == lock_table_.end()) {
    lk.unlock();
    return false;
  }

  // 由于加写锁需要保证当前没有读锁，那么如果队列中有两个更新锁的请求，就会互相等待对方解读锁
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID) {
    // 查找该txn是否还存在
    for (LockRequest lock_request : lock_table_[rid].request_queue_) {
      if (lock_request.txn_id_ == lock_table_[rid].upgrading_ &&
          TransactionManager::GetTransaction(lock_request.txn_id_)->GetState() != TransactionState::ABORTED) {
        lk.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
    }
  }

  // Abort所有新的Txn
  AbortNewTxn(txn, rid, false);

  // 寻找队列中该txn的读锁并删除
  bool flag_has_r_lock = false;
  for (std::list<LockRequest>::iterator item = lock_table_[rid].request_queue_.begin();
       item != lock_table_[rid].request_queue_.end(); ++item) {
    if (item->txn_id_ == txn->GetTransactionId()) {
      flag_has_r_lock = true;
      lock_table_[rid].request_queue_.erase(item);
      break;
    }
  }
  if (!flag_has_r_lock) {
    lk.unlock();
    return false;
  }

  lock_table_[rid].upgrading_ = txn->GetTransactionId();

  // 插入最后一个上锁的tuple后
  if (lock_table_[rid].request_queue_.empty()) {
    lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE, false));
  } else {
    auto r_item = lock_table_[rid].request_queue_.end();
    --r_item;
    bool flag_insert = false;
    for (; r_item != lock_table_[rid].request_queue_.begin(); --r_item) {
      if (r_item->granted_) {
        ++r_item;
        flag_insert = true;
        lock_table_[rid].request_queue_.insert(r_item,
                                               LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE, false));
        break;
      }
    }
    if (!flag_insert) {
      if (r_item->granted_) {
        ++r_item;
      }
      lock_table_[rid].request_queue_.insert(r_item, LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE, false));
    }
  }

  // !!!!!!!!注意：即使此时没上锁也要先加入set，可能后续因为其他RID导致事务被abort，这时set中要有该rid才能成功将等待队列中的锁去除
  // 事务中加入RID
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  std::list<LockRequest>::iterator my_request;
  std::mutex mu;
  std::unique_lock<std::mutex> lck(mu);
  while (!CheckXLockL(txn, rid, &my_request)) {
    // LOG_INFO("txn:[%d] ,rid:[%d]-WaitWLock", txn->GetTransactionId(), rid.GetPageId());
    // 不能上锁,则进行等待
    lk.unlock();

    lock_table_[rid].cv_.wait(lck);

    lk.lock();
    // 检查是否被Abort导致唤醒
    if (CheckAbortedL(txn, rid)) {
      if (lock_table_[rid].upgrading_ == txn->GetTransactionId()) {
        lock_table_[rid].upgrading_ = INVALID_TXN_ID;
      }
      txn->GetSharedLockSet()->erase(rid);
      lk.unlock();
      //  return false;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  // 能够上锁
  my_request->granted_ = true;

  // 应该不会出现这种情况
  assert(lock_table_[rid].upgrading_ == txn->GetTransactionId());
  // 重新设置为invalid
  lock_table_[rid].upgrading_ = INVALID_TXN_ID;

  lk.unlock();
  // LOG_INFO("txn:[%d] ,rid:[%d]-UpgradeWLock", txn->GetTransactionId(), rid.GetPageId());
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // READ_UNCOMMITTED也会释放写锁
  // 注意：即使是abort状态，也要能够释放锁，因为abort会触发事务回滚，从而调用该函数释放锁
  std::unique_lock lk(latch_);
  //  printf("[%d]GTE_LATCH-rid[%d]-UnLock1\n", txn->GetTransactionId(), rid.GetPageId());
  //
  //  printf("[%d]--Unlock-rid[%d]-Try:Wait--  ", txn->GetTransactionId(), rid.GetPageId());
  //  for (auto requst :lock_table_[rid].request_queue_) {
  //    printf("%d ",requst.txn_id_);
  //  }
  //  printf("\n");
  // 寻找队列中该txn的锁
  bool flag_has_r_lock = false;
  for (std::list<LockRequest>::iterator item = lock_table_[rid].request_queue_.begin();
       item != lock_table_[rid].request_queue_.end(); ++item) {
    if (item->txn_id_ == txn->GetTransactionId()) {
      flag_has_r_lock = true;
      // 事务中删除RID
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
      // 因为可能Abort回滚时调用，此时不能修改状态
      // REPEATABLE_READ放锁就修改，READ_COMMITTED放读锁才修改
      if (txn->GetState() == TransactionState::GROWING &&
          (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
           (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && item->lock_mode_ == LockMode::EXCLUSIVE) ||
           (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && item->lock_mode_ == LockMode::EXCLUSIVE))) {
        txn->SetState(TransactionState::SHRINKING);
      }

      lock_table_[rid].request_queue_.erase(item);
      lock_table_[rid].cv_.notify_all();
      //      printf("[%d]--UnLock-rid[%d]-Success\n", txn->GetTransactionId(), rid.GetPageId());
      break;
    }
  }
  //  printf("[%d]UN_LATCH-rid[%d]-UnLock1\n", txn->GetTransactionId(), rid.GetPageId());
  lk.unlock();
  return flag_has_r_lock;
}

}  // namespace bustub
