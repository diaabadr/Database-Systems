//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/index_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
  using KeyType = GenericKey<8>;
  using ValueType = RID;
  using KeyComparator = GenericComparator<8>;

 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  BPlusTreeIndex<KeyType, ValueType, KeyComparator> *GetBPlusTreeIndex() {
    return dynamic_cast<BPlusTreeIndex<KeyType, ValueType, KeyComparator> *>(index_info_->index_.get());
  }

  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  /** Metadata identifying the table that should be scanned. */
  const TableMetadata *table_info_;
  /** Index info identifying the index to be scanned */
  const IndexInfo *index_info_;

  std::unique_ptr<INDEXITERATOR_TYPE> index_iter{nullptr};
};
}  // namespace bustub
