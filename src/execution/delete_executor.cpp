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
  Tuple old_tuple;
  if (child_executor_->Next(&old_tuple, rid)) {
    LockInTuple(*rid);

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
      return true;
    }
  }
  return false;
}

}  // namespace bustub
