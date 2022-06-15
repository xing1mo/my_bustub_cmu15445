//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_vector_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  RID lock_rid;
  Tuple old_tuple;
  if (child_executor_->Next(&old_tuple, rid)) {
    // REPEATABLE_READ隔离级别之前的读锁没放，升级为写锁
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
        exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid)) {
      lock_rid = *rid;
    }

    // 所有其他隔离级别直接加写锁
    if (exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
      lock_rid = *rid;
    }

    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      for (IndexInfo *index_info : index_info_vector_) {
        index_info->index_->DeleteEntry(
            old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            *rid, exec_ctx_->GetTransaction());
        // 维护indexWriteSet，以便后面回滚
        exec_ctx_->GetTransaction()->GetIndexWriteSet()->emplace_back(*rid, plan_->TableOid(), WType::DELETE, old_tuple,
                                                                      index_info->index_oid_, exec_ctx_->GetCatalog());
      }

      // 不放写锁
      //      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      //        exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), lock_rid);
      //      }
      return true;
    }

    // READ_COMMITTED隔离级别时需要立即解锁
    //    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    //      exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), lock_rid);
    //    }
  }
  return false;
}

}  // namespace bustub
