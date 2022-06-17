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

void LockManager::AbortNewTxn(Transaction *txn, const RID &rid, bool IsReadLock, LockRequestQueue *request_queue) {
  // 对于上读锁，所有txn_id大的写锁都要abort
  // 上写锁则abort所有txn_id大的
  std::vector<std::list<LockRequest>::iterator> remove_set;
  bool has_abort = false;
  for (auto lock_request = request_queue->request_queue_.begin(); lock_request != request_queue->request_queue_.end();
       ++lock_request) {
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
    request_queue->request_queue_.erase(lock_request);
  }
  if (has_abort) {
    request_queue->cv_.notify_all();
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
  latch_.lock();

  // 构造空的等待队列
  ConstructQueue(txn, rid);
  // Unordered_map Rehash问题,https://www.cnblogs.com/KatyuMarisaBlog/p/14432005.html?ivk_sa=1024320u
  // 在高并发情况下，如果用map[]访问某个key对应的value值，此时刚好其他地方有插入，会导致无法访问到所有值。（这里一个key对应的value是一个链表，该链表在扩容时拆分到了很多个bucket中，而使用map[]访问不到整个链表）
  LockRequestQueue &request_queue = lock_table_[rid];

  // 释放大锁前先上小锁，防止在死锁预防过程中又有新的Request加入
  std::unique_lock tuple_lk(request_queue.query_latch_);

  latch_.unlock();
  // 查询是否已经有锁了
  for (auto request : request_queue.request_queue_) {
    if (request.txn_id_ == txn->GetTransactionId()) {
      tuple_lk.unlock();
      return true;
    }
  }

  request_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED, false));
  // !!!!!!!!注意：即使此时没上锁也要先加入set，可能后续因为其他RID导致事务被abort，这时set中要有该rid才能成功将等待队列中的锁去除
  // 事务中加入RID
  txn->GetSharedLockSet()->emplace(rid);

  // Abort所有新的Txn
  AbortNewTxn(txn, rid, true, &request_queue);

  std::list<LockRequest>::iterator my_request;

  // !!!注意不同的wait要声明不同的mutex
  std::mutex mu;
  std::unique_lock<std::mutex> lck(mu);
  while (!CheckSLockL(txn, rid, &my_request, &request_queue)) {
    // LOG_INFO("txn:[%d] ,rid:[%d]-WaitRLock", txn->GetTransactionId(), rid.GetPageId());
    // 不能上锁,则进行等待
    tuple_lk.unlock();
    request_queue.cv_.wait(lck);

    tuple_lk.lock();
    // 检查是否被Abort导致唤醒
    if (CheckAbortedL(txn, rid)) {
      tuple_lk.unlock();
      return false;
    }
  }
  // 能够上锁
  my_request->granted_ = true;
  tuple_lk.unlock();
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 判断异常
  if (!PreCheckLock(txn, rid)) {
    return false;
  }

  // 整个加锁，避免同时构造导致覆盖掉刚新建的lock_table_
  latch_.lock();

  // 构造空的等待队列
  ConstructQueue(txn, rid);
  // Unordered_map Rehash问题,https://www.cnblogs.com/KatyuMarisaBlog/p/14432005.html?ivk_sa=1024320u
  // 在高并发情况下，如果用map[]访问某个key对应的value值，此时刚好其他地方有插入，会导致无法访问到所有值。（这里一个key对应的value是一个链表，该链表在扩容时拆分到了很多个bucket中，而使用map[]访问不到整个链表）
  LockRequestQueue &request_queue = lock_table_[rid];

  // 释放大锁前先上小锁，防止在死锁预防过程中又有新的Request加入
  std::unique_lock tuple_lk(request_queue.query_latch_);

  latch_.unlock();
  // 查询是否已经有锁了
  for (auto request : request_queue.request_queue_) {
    if (request.txn_id_ == txn->GetTransactionId()) {
      tuple_lk.unlock();
      return true;
    }
  }
  request_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE, false));
  // !!!!!!!!注意：即使此时没上锁也要先加入set，可能后续因为其他RID导致事务被abort，这时set中要有该rid才能成功将等待队列中的锁去除
  // 事务中加入RID
  txn->GetExclusiveLockSet()->emplace(rid);

  // Abort所有新的Txn
  AbortNewTxn(txn, rid, false, &request_queue);

  std::list<LockRequest>::iterator my_request;
  std::mutex mu;
  std::unique_lock<std::mutex> lck(mu);
  while (!CheckXLockL(txn, rid, &my_request, &request_queue)) {
    // LOG_INFO("txn:[%d] ,rid:[%d]-WaitWLock", txn->GetTransactionId(), rid.GetPageId());
    // 不能上锁,则进行等待
    tuple_lk.unlock();

    request_queue.cv_.wait(lck);

    tuple_lk.lock();
    // 检查是否被Abort导致唤醒
    if (CheckAbortedL(txn, rid)) {
      tuple_lk.unlock();
      //  return false;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  // 能够上锁
  my_request->granted_ = true;
  tuple_lk.unlock();
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 判断异常
  if (!PreCheckLock(txn, rid)) {
    return false;
  }

  // 整个加锁
  latch_.lock();
  if (lock_table_.find(rid) == lock_table_.end()) {
    latch_.unlock();
    return false;
  }
  LockRequestQueue &request_queue = lock_table_[rid];
  // 释放大锁前先上小锁，防止在死锁预防过程中又有新的Request加入
  std::unique_lock tuple_lk(request_queue.query_latch_);
  latch_.unlock();

  // 由于加写锁需要保证当前没有读锁，那么如果队列中有两个更新锁的请求，就会互相等待对方解读锁
  if (request_queue.upgrading_ != INVALID_TXN_ID) {
    // 查找该txn是否还存在
    for (LockRequest lock_request : request_queue.request_queue_) {
      if (lock_request.txn_id_ == request_queue.upgrading_ &&
          TransactionManager::GetTransaction(lock_request.txn_id_)->GetState() != TransactionState::ABORTED) {
        tuple_lk.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
    }
  }

  // Abort所有新的Txn
  AbortNewTxn(txn, rid, false, &request_queue);

  // 寻找队列中该txn的读锁并删除
  bool flag_has_r_lock = false;
  for (std::list<LockRequest>::iterator item = request_queue.request_queue_.begin();
       item != request_queue.request_queue_.end(); ++item) {
    if (item->txn_id_ == txn->GetTransactionId()) {
      flag_has_r_lock = true;
      request_queue.request_queue_.erase(item);
      break;
    }
  }
  if (!flag_has_r_lock) {
    tuple_lk.unlock();
    return false;
  }

  request_queue.upgrading_ = txn->GetTransactionId();

  // 插入最后一个上锁的tuple后
  if (request_queue.request_queue_.empty()) {
    request_queue.request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE, false));
  } else {
    auto r_item = request_queue.request_queue_.end();
    --r_item;
    bool flag_insert = false;
    for (; r_item != request_queue.request_queue_.begin(); --r_item) {
      if (r_item->granted_) {
        ++r_item;
        flag_insert = true;
        request_queue.request_queue_.insert(r_item, LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE, false));
        break;
      }
    }
    if (!flag_insert) {
      if (r_item->granted_) {
        ++r_item;
      }
      request_queue.request_queue_.insert(r_item, LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE, false));
    }
  }

  // !!!!!!!!注意：即使此时没上锁也要先加入set，可能后续因为其他RID导致事务被abort，这时set中要有该rid才能成功将等待队列中的锁去除
  // 事务中加入RID
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  std::list<LockRequest>::iterator my_request;
  std::mutex mu;
  std::unique_lock<std::mutex> lck(mu);
  while (!CheckXLockL(txn, rid, &my_request, &request_queue)) {
    // LOG_INFO("txn:[%d] ,rid:[%d]-WaitWLock", txn->GetTransactionId(), rid.GetPageId());
    // 不能上锁,则进行等待
    tuple_lk.unlock();

    request_queue.cv_.wait(lck);

    tuple_lk.lock();
    // 检查是否被Abort导致唤醒
    if (CheckAbortedL(txn, rid)) {
      if (request_queue.upgrading_ == txn->GetTransactionId()) {
        request_queue.upgrading_ = INVALID_TXN_ID;
      }
      txn->GetSharedLockSet()->erase(rid);
      tuple_lk.unlock();
      //  return false;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  // 能够上锁
  my_request->granted_ = true;

  // 应该不会出现这种情况
  assert(request_queue.upgrading_ == txn->GetTransactionId());
  // 重新设置为invalid
  request_queue.upgrading_ = INVALID_TXN_ID;

  tuple_lk.unlock();
  // LOG_INFO("txn:[%d] ,rid:[%d]-UpgradeWLock", txn->GetTransactionId(), rid.GetPageId());
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // READ_UNCOMMITTED也会释放写锁
  // 注意：即使是abort状态，也要能够释放锁，因为abort会触发事务回滚，从而调用该函数释放锁
  // 整个加锁
  latch_.lock();
  LockRequestQueue &request_queue = lock_table_[rid];
  // 释放大锁前先上小锁，防止在死锁预防过程中又有新的Request加入
  std::unique_lock tuple_lk(request_queue.query_latch_);
  latch_.unlock();

  // 寻找队列中该txn的锁
  bool flag_has_r_lock = false;
  for (std::list<LockRequest>::iterator item = request_queue.request_queue_.begin();
       item != request_queue.request_queue_.end(); ++item) {
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

      request_queue.request_queue_.erase(item);
      request_queue.cv_.notify_all();
      break;
    }
  }
  tuple_lk.unlock();
  return flag_has_r_lock;
}

}  // namespace bustub
