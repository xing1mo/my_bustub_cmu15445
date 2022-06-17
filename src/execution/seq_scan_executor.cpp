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
  while (iter_ != table_info_->table_->End()) {
    *rid = iter_->GetRid();

    // iter是在++的时候读取新值，这个时候我们用不到，只需要他的RID，因此重新加锁读取
    *tuple = *iter_;
    LockInTuple(*rid);
    bool res = table_info_->table_->GetTuple(*rid, tuple, GetExecutorContext()->GetTransaction());
    UnlockInTuple(*rid);

    if (!res) {
      iter_++;
      continue;
    }
    if (plan_->GetPredicate() != nullptr &&
        !plan_->GetPredicate()->Evaluate(tuple, &table_info_->schema_).GetAs<bool>()) {
      iter_++;
    } else {
      break;
    }
  }
  if (iter_ == table_info_->table_->End()) {
    return false;
  }

  std::vector<Value> res;
  const Schema *output_schema = GetOutputSchema();  // output_schema是返回格式
  res.reserve(output_schema->GetColumnCount());

  for (const Column &column : output_schema->GetColumns()) {
    // 获取tuple中这一列的值，第二个参数schema是Tuple原有的格式
    res.push_back(column.GetExpr()->Evaluate(tuple, &table_info_->schema_));
  }
  *tuple = Tuple(res, output_schema);  // 调用Tuple的构造函数
  *rid = iter_->GetRid();
  iter_++;
  return true;
}

}  // namespace bustub
