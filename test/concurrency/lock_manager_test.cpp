/**
 * lock_manager_test.cpp
 */

#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

#include <atomic>
#include <future>  //NOLINT
#include <thread>  //NOLINT

#include "common/logger.h"
#include "concurrency/transaction.h"

#define random(a, b) ((a) + rand() % ((b) - (a) + 1))

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

// Basic shared lock test under REPEATABLE_READ
void BasicTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 10;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };
  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, BasicTest) { BasicTest1(); }

void TwoPLTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{0, 1};

  auto txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockShared(txn, rid0);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 0);

  res = lock_mgr.LockExclusive(txn, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 1);

  res = lock_mgr.Unlock(txn, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 1);

  try {
    lock_mgr.LockShared(txn, rid0);
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  } catch (TransactionAbortException &e) {
    // std::cout << e.GetInfo() << std::endl;
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnLockSize(txn, 0, 0);

  delete txn;
}
TEST(LockManagerTest, TwoPLTest) { TwoPLTest(); }

void UpgradeTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn(0);
  txn_mgr.Begin(&txn);

  bool res = lock_mgr.LockShared(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 1, 0);
  CheckGrowing(&txn);

  res = lock_mgr.LockUpgrade(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 1);
  CheckGrowing(&txn);

  res = lock_mgr.Unlock(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 0);
  CheckShrinking(&txn);

  txn_mgr.Commit(&txn);
  CheckCommitted(&txn);
}
TEST(LockManagerTest, UpgradeLockTest) { UpgradeTest(); }

void WoundWaitBasicTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  int id_hold = 0;
  int id_die = 1;

  std::promise<void> t1done;
  std::shared_future<void> t1_future(t1done.get_future());

  auto wait_die_task = [&]() {
    // younger transaction acquires lock first
    Transaction txn_die(id_die);
    txn_mgr.Begin(&txn_die);
    bool res = lock_mgr.LockExclusive(&txn_die, rid);
    EXPECT_TRUE(res);

    CheckGrowing(&txn_die);
    CheckTxnLockSize(&txn_die, 0, 1);

    t1done.set_value();

    // wait for txn 0 to call lock_exclusive(), which should wound us
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    CheckAborted(&txn_die);

    // unlock
    txn_mgr.Abort(&txn_die);
  };

  Transaction txn_hold(id_hold);
  txn_mgr.Begin(&txn_hold);

  // launch the waiter thread
  std::thread wait_thread{wait_die_task};

  // wait for txn1 to lock
  t1_future.wait();

  bool res = lock_mgr.LockExclusive(&txn_hold, rid);
  EXPECT_TRUE(res);

  wait_thread.join();

  CheckGrowing(&txn_hold);
  txn_mgr.Commit(&txn_hold);
  CheckCommitted(&txn_hold);
}
TEST(LockManagerTest, WoundWaitBasicTest) { WoundWaitBasicTest(); }

// --- Real tests ---
// Basic shared lock test under REPEATABLE_READ
void BasicTest4() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 100;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };
  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}

// Basic shared lock test under READ_COMMITTED
void BasicTest2() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 100;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin(nullptr, IsolationLevel::READ_COMMITTED));
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };
  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}

// Basic shared lock test under READ_UNCOMMITTED
void BasicTest3() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 100;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin(nullptr, IsolationLevel::READ_UNCOMMITTED));
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    for (const RID &rid : rids) {
      try {
        lock_mgr.LockShared(txns[txn_id], rid);
      } catch (TransactionAbortException &e) {
        CheckAborted(txns[txn_id]);
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}

// Correct case

/****************************
 * Basic Tests (15 pts)
 ****************************/

const size_t NUM_ITERS = 100;

/*
 * Score: 5
 * Description: Basic tests for LockShared and Unlock operations
 * on small amount of rids.
 */
TEST(LockManagerTest, BasicTest1) {
  //  TEST_TIMEOUT_BEGIN
  for (size_t i = 0; i < NUM_ITERS; i++) {
    BasicTest4();
    BasicTest2();
    BasicTest3();
  }
  //  TEST_TIMEOUT_FAIL_END(1000 * 50)
}

// 检测高并法下是否出现死锁
void MyWoundWaitTest() {
  int seed = time(nullptr);
  srand(seed);

  //  freopen("/home/xingmo/test_out.out","w",stdout);

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  //  auto detect = [&]() {
  //    while (true){
  //      if (lock_mgr.GetMyLatch()){
  //        lock_mgr.UnLockLatch();
  //        printf("no-dead\n");
  //      }else{
  //        printf("dead\n");
  //      }
  //      std::this_thread::sleep_for(std::chrono::milliseconds(50));
  //    }
  //  };
  //  std::thread dct{detect};
  //  dct.detach();

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 10;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin(nullptr, IsolationLevel::REPEATABLE_READ));
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test
  std::mutex mu;
  std::unique_lock<std::mutex> lck(mu);
  std::condition_variable cv;
  int finish_cnt = 0;
  std::mutex latch;

  auto task = [&](int txn_id) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    //    bool has_abort = false;
    int cnt_s = 0;
    int cnt_x = 0;

    //随机打乱数组
    std::vector<RID> old_rids;
    for (const RID &rid : rids) {
      old_rids.push_back(rid);
    }
    std::vector<RID> new_rids;
    for (int i = num_rids; i > 0; i--) {
      int index = random(0, i - 1);
      //根据选中的下标将原数组选中
      // push_back函数，在vector类中作用为在vector尾部加入一个数据。
      new_rids.push_back(old_rids[index]);
      //将原数组中选中的元素剔除
      old_rids.erase(old_rids.begin() + index);
    }

    //    do {
    //      has_abort = false;
    cnt_x = cnt_s = 0;
    try {
      //        printf("\n[%d]--BEGIN-Lock\n",txn_id);
      for (const RID &rid : new_rids) {
        bool flag = false;
        int type = random(0, 1);

        if (type == 0) {
          flag = lock_mgr.LockShared(txns[txn_id], rid);
          if (flag) {
            ++cnt_s;
            //              printf("[%d]--SLock-rid[%d]-Success\n", txn_id, rid.GetPageId());
          }
        } else {
          //            printf("[%d]--XLock-rid[%d]-Try\n", txn_id, rid.GetPageId());
          flag = lock_mgr.LockExclusive(txns[txn_id], rid);
          if (flag) {
            ++cnt_x;
            //              printf("[%d]--XLock-rid[%d]-Success\n", txn_id, rid.GetPageId());
          }
        }
      }

      printf("\n[%d]--BEGIN-UnLock\n", txn_id);
      for (const RID &rid : new_rids) {
        //          printf("[%d]--UnLock-rid[%d]\n",txn_id,rid.GetPageId());
        lock_mgr.Unlock(txns[txn_id], rid);
      }

      if (txns[txn_id]->GetState() == TransactionState::ABORTED) {
        printf("\n[%d]--Abort\n", txn_id);
        txn_mgr.Abort(txns[txn_id]);
        //          has_abort = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(random(50, 200)));
      } else {
        printf("\n[%d]--End-UnLock\n", txn_id);
        txn_mgr.Commit(txns[txn_id]);
        CheckCommitted(txns[txn_id]);
      }

    } catch (TransactionAbortException &e) {
      std::cout << e.GetInfo() << std::endl;
      CheckAborted(txns[txn_id]);
      printf("\n[%d]--Abort\n", txn_id);
      txn_mgr.Abort(txns[txn_id]);
      //        has_abort = true;
      std::this_thread::sleep_for(std::chrono::milliseconds(random(50, 200)));
    }

    //    } while (has_abort);
    //      printf("be-lock\n");
    latch.lock();
    //      printf("end-lock\n");
    ++finish_cnt;
    //    printf("\nfinishcnt-%d\n\n",finish_cnt);
    cv.notify_all();
    latch.unlock();
  };

  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    std::thread tt{task, i};
    tt.detach();
  }

  //  printf("be-lock\n");
  latch.lock();
  //  printf("end-lock\n");
  while (finish_cnt < num_rids) {
    printf("\nfinishcnt-%d\n\n", finish_cnt);
    latch.unlock();
    cv.wait(lck);
    //    printf("be-lock\n");
    latch.lock();
    //    printf("end-lock\n");
  }

  printf("\nfinishcnt-end-%d\n\n", finish_cnt);
  latch.unlock();
  //
  //  for (int i = 0; i < num_rids; i++) {
  //    threads[i].join();
  //  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, MyWoundWaitTest) { MyWoundWaitTest(); }

}  // namespace bustub
