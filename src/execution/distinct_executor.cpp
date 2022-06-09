//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  // 将所有tuple放入hash表
  const Schema *output_schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> res;
    res.reserve(output_schema->GetColumnCount());
    for (size_t i = 0; i < output_schema->GetColumnCount(); ++i) {
      res.push_back(tuple.GetValue(output_schema, i));
    }
    //    for (const Column &column : output_schema->GetColumns()) {
    //      res.push_back(column.GetExpr()->Evaluate(&tuple, output_schema));
    //    }
    if (distinct_hash_table_.count(DistinctKey{res}) == 0) {
      distinct_hash_table_.insert({DistinctKey{res}, rid});
    }
  }
  iter_ = distinct_hash_table_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == distinct_hash_table_.end()) {
    return false;
  }
  // 感觉有可能最终的output并不是去重后的tuple
  //  Tuple tmp_tuple = Tuple(iter_->first.tuples_,child_executor_->GetOutputSchema());
  *tuple = Tuple(iter_->first.tuples_, child_executor_->GetOutputSchema());
  *rid = iter_->second;
  iter_++;
  // 生成最终结果
  //  std::vector<Value> res;
  //  const Schema *output_schema = GetOutputSchema(); // output_schema是返回格式
  //  res.reserve(output_schema->GetColumnCount());
  //  for (const Column &column : output_schema->GetColumns()) {
  //    res.push_back(tmp_tuple.GetValue(child_executor_->GetOutputSchema(),column.))
  //    // 获取tuple中这一列的值，第二个参数schema是Tuple原有的格式
  //    res.push_back(column.GetExpr()->Evaluate(&tmp_tuple, plan_->GetChildPlan()->OutputSchema()));
  //  }
  //  *tuple = Tuple(res, output_schema); // 调用Tuple的构造函数

  return true;
}

}  // namespace bustub
