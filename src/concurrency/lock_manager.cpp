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

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 判断异常
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::ABORTED){
    return false;
  }

  // 整个加锁，避免同时构造导致覆盖掉刚新建的lock_table_
  latch_.lock();
  // 构造空的等待队列
  if (lock_table_.find(rid) == lock_table_.end()){
    // mutex和condition_variable不能被复制或移动,只能采取在map中原地构造的方式将其加入，即使用emplace()，并且需要配合pair’s piecewise constructor。https://www.cnblogs.com/guxuanqing/p/11396511.html
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
  lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(),LockMode::SHARED, false));
  latch_.unlock();

  // 所有txn_id大的写锁都要abort
  bool has_abort = false;
  lock_table_[rid].query_latch_.lock();
  for (LockRequest lock_request :lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ > txn->GetTransactionId() && lock_request.lock_mode_ == LockMode::EXCLUSIVE){
      TransactionManager::GetTransaction(lock_request.txn_id_)->SetState(TransactionState::ABORTED);
      has_abort = true;
    }
  }
  if (has_abort){
    lock_table_[rid].cv_.notify_all();
  }
  lock_table_[rid].query_latch_.unlock();

  while(true){
    lock_table_[rid].query_latch_.lock();

    // 判断是否能够上读锁
    bool flag = true;
//    bool assert_find_me = false;
    // 获取加入队列时的位置
    std::list<LockRequest>::iterator my_request;
    for (std::list<LockRequest>::iterator item = lock_table_[rid].request_queue_.begin();
         item != lock_table_[rid].request_queue_.end();++item) {
      if (item->txn_id_ == txn->GetTransactionId()){
        my_request = item;
//        assert_find_me = true;
        break ;
      }
      if (item->granted_ && item->lock_mode_ == LockMode::EXCLUSIVE){
        flag = false;
        break ;
      }
    }

    // 能够上锁
    if (flag){
//      assert(assert_find_me);
      // 将该请求标记为已上锁，采用加锁在前，为加锁在后的方式优化
      lock_table_[rid].request_queue_.erase(my_request);
      lock_table_[rid].request_queue_.emplace_front(LockRequest(txn->GetTransactionId(),LockMode::SHARED, true));
      // 事务中加入RID
      txn->GetSharedLockSet()->emplace(rid);
      lock_table_[rid].query_latch_.unlock();
      LOG_INFO("txn:[%d] ,rid:[%d]-GetRLock",txn->GetTransactionId(),rid.GetPageId());
      return true;
    }

    lock_table_[rid].query_latch_.unlock();
    LOG_INFO("txn:[%d] ,rid:[%d]-WaitRLock",txn->GetTransactionId(),rid.GetPageId());
    // 不能上锁,则进行等待
    std::unique_lock <std::mutex> lck(lock_table_[rid].mutex_);
    lock_table_[rid].cv_.wait(lck);

    // 被abort唤醒返回false
    if (txn->GetState() == TransactionState::ABORTED){
      return false;
    }
  }

}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 判断异常
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::ABORTED){
    return false;
  }

  // 整个加锁，避免同时构造导致覆盖掉刚新建的lock_table_
  latch_.lock();
  // 构造空的等待队列
  if (lock_table_.find(rid) == lock_table_.end()){
    // mutex和condition_variable不能被复制或移动,只能采取在map中原地构造的方式将其加入，即使用emplace()，并且需要配合pair’s piecewise constructor。https://www.cnblogs.com/guxuanqing/p/11396511.html
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
  lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(),LockMode::EXCLUSIVE, false));
  latch_.unlock();

  // 所有txn_id大的都要abort
  bool has_abort = false;
  lock_table_[rid].query_latch_.lock();
  for (LockRequest lock_request :lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ > txn->GetTransactionId()){
      TransactionManager::GetTransaction(lock_request.txn_id_)->SetState(TransactionState::ABORTED);
      has_abort = true;
    }
  }
  if (has_abort){
    lock_table_[rid].cv_.notify_all();
  }
  lock_table_[rid].query_latch_.unlock();

  while(true){
    lock_table_[rid].query_latch_.lock();

    // 判断是否能够上写锁

    // 能够上锁，队列最前的都没上锁，说明全没上锁
    if (!lock_table_[rid].request_queue_.front().granted_){
      // 查找一开始添加的位置
      std::list<LockRequest>::iterator my_request = lock_table_[rid].request_queue_.begin();
      for (;my_request != lock_table_[rid].request_queue_.end();++my_request) {
        if (my_request->txn_id_ == txn->GetTransactionId()){
          break ;
        }
      }
      // 将该请求标记为已上锁，采用加锁在前，为加锁在后的方式优化
      lock_table_[rid].request_queue_.erase(my_request);
      lock_table_[rid].request_queue_.emplace_front(LockRequest(txn->GetTransactionId(),LockMode::SHARED, true));

      // 事务中加入RID
      txn->GetExclusiveLockSet()->emplace(rid);
      lock_table_[rid].query_latch_.unlock();
      LOG_INFO("txn:[%d] ,rid:[%d]-GetWLock",txn->GetTransactionId(),rid.GetPageId());
      return true;
    }

    lock_table_[rid].query_latch_.unlock();
    LOG_INFO("txn:[%d] ,rid:[%d]-WaitWLock",txn->GetTransactionId(),rid.GetPageId());
    // 不能上锁,则进行等待
    std::unique_lock <std::mutex> lck(lock_table_[rid].mutex_);
    lock_table_[rid].cv_.wait(lck);

    // 被abort唤醒返回false
    if (txn->GetState() == TransactionState::ABORTED){
      return false;
    }
  }
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 判断异常
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::ABORTED){
    return false;
  }

  if (lock_table_.find(rid) == lock_table_.end()){
    return false;
  }

  lock_table_[rid].query_latch_.lock();
  // 寻找队列中该txn的读锁
  bool flag_has_r_lock = false;
  for (std::list<LockRequest>::iterator item = lock_table_[rid].request_queue_.begin();
       item != lock_table_[rid].request_queue_.end();++item) {
    if (item->txn_id_ == txn->GetTransactionId()){
      flag_has_r_lock = true;
      lock_table_[rid].request_queue_.erase(item);
      break ;
    }
  }
  if (!flag_has_r_lock){
    lock_table_[rid].query_latch_.unlock();
    return false;
  }

  // 由于加写锁需要保证当前没有读锁，那么如果队列中有两个更新锁的请求，就会互相等待对方解读锁
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID){
    lock_table_[rid].query_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::UPGRADE_CONFLICT);
  }
  lock_table_[rid].upgrading_ = txn->GetTransactionId();
  lock_table_[rid].query_latch_.unlock();

  lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(),LockMode::EXCLUSIVE, false));

  // 所有txn_id大的都要abort
  bool has_abort = false;
  lock_table_[rid].query_latch_.lock();
  for (LockRequest lock_request :lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ > txn->GetTransactionId()){
      TransactionManager::GetTransaction(lock_request.txn_id_)->SetState(TransactionState::ABORTED);
      has_abort = true;
    }
  }
  if (has_abort){
    lock_table_[rid].cv_.notify_all();
  }
  lock_table_[rid].query_latch_.unlock();

  while(true){
    lock_table_[rid].query_latch_.lock();

    // 判断是否能够上写锁

    // 能够上锁，队列最前的都没上锁，说明全没上锁
    if (!lock_table_[rid].request_queue_.front().granted_){
      // 查找一开始添加的位置
      std::list<LockRequest>::iterator my_request = lock_table_[rid].request_queue_.begin();
      for (;my_request != lock_table_[rid].request_queue_.end();++my_request) {
        if (my_request->txn_id_ == txn->GetTransactionId()){
          break ;
        }
      }
      // 将该请求标记为已上锁，采用加锁在前，为加锁在后的方式优化
      lock_table_[rid].request_queue_.erase(my_request);
      lock_table_[rid].request_queue_.emplace_front(LockRequest(txn->GetTransactionId(),LockMode::SHARED, true));

      // 应该不会出现这种情况
      assert(lock_table_[rid].upgrading_ == txn->GetTransactionId());
      // 重新设置为invalid
      lock_table_[rid].upgrading_ = INVALID_TXN_ID;
      // 事务中加入RID
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->emplace(rid);
      lock_table_[rid].query_latch_.unlock();
      LOG_INFO("txn:[%d] ,rid:[%d]-UpgradeWLock",txn->GetTransactionId(),rid.GetPageId());
      return true;
    }

    lock_table_[rid].query_latch_.unlock();
    LOG_INFO("txn:[%d] ,rid:[%d]-Wait-UpgradeWLock",txn->GetTransactionId(),rid.GetPageId());
    // 不能上锁,则进行等待
    std::unique_lock <std::mutex> lck(lock_table_[rid].mutex_);
    lock_table_[rid].cv_.wait(lck);

    // 被abort唤醒返回false
    if (txn->GetState() == TransactionState::ABORTED){
      return false;
    }
  }
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 判断异常
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  // 注意：即使是abort状态，也要能够释放锁，因为abort会触发事务回滚，从而调用该函数释放锁
  lock_table_[rid].query_latch_.lock();
  // 寻找队列中该txn的锁
  bool flag_has_r_lock = false;
  for (std::list<LockRequest>::iterator item = lock_table_[rid].request_queue_.begin();
       item != lock_table_[rid].request_queue_.end();++item) {
    if (item->txn_id_ == txn->GetTransactionId()){
      flag_has_r_lock = true;
      lock_table_[rid].request_queue_.erase(item);
      // 事务中删除RID
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
      //因为可能Abort回滚时调用，此时不能修改状态
      if (txn->GetState() == TransactionState::GROWING){
        txn->SetState(TransactionState::SHRINKING);
      }

      lock_table_[rid].cv_.notify_all();
      LOG_INFO("txn:[%d] ,rid:[%d]-UnLock",txn->GetTransactionId(),rid.GetPageId());
      break ;
    }
  }
  lock_table_[rid].query_latch_.unlock();
  return flag_has_r_lock;
}

}  // namespace bustub
