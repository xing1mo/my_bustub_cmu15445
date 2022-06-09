//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  key_to_tuple_table_.clear();
  Tuple tuple;
  RID rid;
  is_end_ = true;
  while (left_executor_->Next(&tuple, &rid)) {
    JoinKey key = JoinKey{plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_executor_->GetOutputSchema())};
    if (key_to_tuple_table_.count(key) == 0) {
      key_to_tuple_table_.insert({key, JoinValue{std::vector<Tuple>{tuple}}});
    } else {
      key_to_tuple_table_[key].tuples_.emplace_back(tuple);
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // 找到下一个right tuple
  while (is_end_) {
    if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
      return false;
    }
    right_key_ = JoinKey{plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_executor_->GetOutputSchema())};
    if (key_to_tuple_table_.count(right_key_) != 0) {
      is_end_ = false;
      iter_ = key_to_tuple_table_[right_key_].tuples_.begin();
    }
  }
  // 合并
  std::vector<Value> res;
  const Schema *output_schema = GetOutputSchema();  // output_schema是返回格式
  res.reserve(output_schema->GetColumnCount());
  for (const Column &column : output_schema->GetColumns()) {
    res.push_back(column.GetExpr()->EvaluateJoin(&(*iter_), left_executor_->GetOutputSchema(), &right_tuple_,
                                                 right_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(res, output_schema);
  iter_++;
  if (iter_ == key_to_tuple_table_[right_key_].tuples_.end()) {
    is_end_ = true;
  }
  return true;
}

}  // namespace bustub
