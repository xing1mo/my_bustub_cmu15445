//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 * Select plan_->OutputSchema() From Table Where ...
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;

  TableInfo *table_info_;
  TableIterator iter_ = TableIterator(nullptr, bustub::RID(), nullptr);

  bool LockInTuple(const RID &rid) {
    Transaction *txn = GetExecutorContext()->GetTransaction();
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      return false;
    }
    if (txn->GetExclusiveLockSet()->find(rid) != txn->GetExclusiveLockSet()->end() ||
        txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      return false;
    }
    // 非READ_UNCOMMITTED隔离级别时需要加锁
    GetExecutorContext()->GetLockManager()->LockShared(txn, rid);
    return true;
  }

  // READ_COMMITTED隔离级别时需要立即解锁
  void UnlockInTuple(const RID &rid) {
    Transaction *txn = GetExecutorContext()->GetTransaction();
    // If in READ_COMMITTED isolation level and txn never lock this Record before this read
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        txn->GetExclusiveLockSet()->find(rid) == txn->GetExclusiveLockSet()->end()) {
      GetExecutorContext()->GetLockManager()->Unlock(txn, rid);
    }
  }
};
}  // namespace bustub
