//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aggregation_hash_table_(plan->GetAggregates(), plan->GetAggregateTypes()),
      iter_(aggregation_hash_table_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aggregation_hash_table_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  iter_ = aggregation_hash_table_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == aggregation_hash_table_.End()) {
    return false;
  }
  // 找到满足having条件的iter
  while (plan_->GetHaving() != nullptr &&
         !plan_->GetHaving()->EvaluateAggregate(iter_.Key().group_bys_, iter_.Val().aggregates_).GetAs<bool>()) {
    ++iter_;
    if (iter_ == aggregation_hash_table_.End()) {
      return false;
    }
  }

  std::vector<Value> res;
  const Schema *output_schema = GetOutputSchema();
  res.reserve(output_schema->GetColumnCount());
  for (const Column &column : output_schema->GetColumns()) {
    // 获取tuple中这一列的值，第二个参数schema是Tuple原有的格式
    res.push_back(column.GetExpr()->EvaluateAggregate(iter_.Key().group_bys_, iter_.Val().aggregates_));
  }
  *tuple = Tuple(res, output_schema);
  ++iter_;
  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
