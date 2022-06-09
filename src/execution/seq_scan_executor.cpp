//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // 筛选满足条件的tuple
  while (iter_ != table_info_->table_->End() && plan_->GetPredicate() != nullptr &&
         !plan_->GetPredicate()->Evaluate(&(*iter_), &table_info_->schema_).GetAs<bool>()) {
    ++iter_;
  }
  if (iter_ == table_info_->table_->End()) {
    return false;
  }

  std::vector<Value> res;
  const Schema *output_schema = GetOutputSchema();  // output_schema是返回格式
  res.reserve(output_schema->GetColumnCount());
  for (const Column &column : output_schema->GetColumns()) {
    // 获取tuple中这一列的值，第二个参数schema是Tuple原有的格式
    res.push_back(column.GetExpr()->Evaluate(&(*iter_), &table_info_->schema_));
  }
  *tuple = Tuple(res, output_schema);  // 调用Tuple的构造函数
  *rid = (*iter_).GetRid();
  iter_++;  // iter_指向下一个Tuple

  return true;
}

}  // namespace bustub
