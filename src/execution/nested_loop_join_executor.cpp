//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  begin_flag_ = true;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  do {
    if (begin_flag_ && !left_executor_->Next(&left_tuple_, &left_rid_)) {
      return false;
    }
    begin_flag_ = false;
    do {
      if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
        if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
          return false;
        }
        right_executor_->Init();
      } else {
        break;
      }
    } while (true);
  } while (plan_->Predicate() != nullptr && !plan_->Predicate()
                                                 ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                                &right_tuple_, right_executor_->GetOutputSchema())
                                                 .GetAs<bool>());

  std::vector<Value> res;
  const Schema *output_schema = GetOutputSchema();  // output_schema是返回格式
  res.reserve(output_schema->GetColumnCount());
  for (const Column &column : output_schema->GetColumns()) {
    res.push_back(column.GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                 right_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(res, output_schema);  // 调用Tuple的构造函数

  return true;
}

}  // namespace bustub
