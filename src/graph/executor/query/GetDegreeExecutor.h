/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_GETDEGREEEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETDEGREEEXECUTOR_H_

#include "common/base/StatusOr.h"
#include "graph/executor/StorageAccessExecutor.h"

namespace nebula {
namespace graph {

class GetOutdegreeExecutor final : public StorageAccessExecutor {
 public:
  GetOutdegreeExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("GetOutdegreeExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class GetIndegreeExecutor final : public StorageAccessExecutor {
 public:
  GetIndegreeExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("GetIndegreeExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETDEGREEEXECUTOR_H_
