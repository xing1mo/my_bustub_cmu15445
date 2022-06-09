//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_vector_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  iter_ = 0;
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 注意不能返回tuple!!!!!!!
  Tuple insert_tuple;
  if (!plan_->IsRawInsert()) {
    if (child_executor_->Next(&insert_tuple, rid)) {
      // 可能错误点：插入失败后续也要继续插入
      if (table_info_->table_->InsertTuple(insert_tuple, rid, exec_ctx_->GetTransaction())) {
        for (IndexInfo *index_info : index_info_vector_) {
          index_info->index_->InsertEntry(insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                                    index_info->index_->GetKeyAttrs()),
                                          *rid, exec_ctx_->GetTransaction());
        }
      }
      return true;
    }
    return false;
  }
  if (iter_ >= plan_->RawValues().size()) {
    return false;
  }
  // table_schema是表本身的格式
  const Schema *table_schema = &table_info_->schema_;
  insert_tuple = Tuple(plan_->RawValuesAt(iter_), table_schema);
  ++iter_;
  if (table_info_->table_->InsertTuple(insert_tuple, rid, exec_ctx_->GetTransaction())) {
    for (IndexInfo *index_info : index_info_vector_) {
      index_info->index_->InsertEntry(
          insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, exec_ctx_->GetTransaction());
    }
  }
  return true;
}

}  // namespace bustub
