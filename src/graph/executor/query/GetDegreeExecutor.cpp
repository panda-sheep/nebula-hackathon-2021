/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/GetDegreeExecutor.h"

#include "common/time/ScopedTimer.h"
#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;
using nebula::storage::cpp2::GetDegreeResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetOutdegreeExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *outdegreeNode = asNode<GetOutdegree>(node());

  time::Duration getOutdegreeTime;
  StorageClient::CommonRequestParam param(outdegreeNode->getSpaceId(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return qctx()
      ->getStorageClient()
      ->getDegree(param, outdegreeNode->getVId(), outdegreeNode->getEdgeType())
      .via(runner())
      .ensure([getOutdegreeTime]() {
        VLOG(1) << "Get outdegree time: " << getOutdegreeTime.elapsedInUSec() << "us";
      })
      .thenValue([this](StatusOr<GetDegreeResponse> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        auto value = std::move(resp).value();
        for (auto &code : value.get_result().get_failed_parts()) {
          NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
        }

        auto data = *value.props_ref();
        return finish(
            ResultBuilder().value(std::move(data)).iter(Iterator::Kind::kDefault).build());
      });
}

folly::Future<Status> GetIndegreeExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *indegreeNode = asNode<GetIndegree>(node());

  time::Duration getIndegreeTime;
  StorageClient::CommonRequestParam param(indegreeNode->getSpaceId(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return qctx()
      ->getStorageClient()
      ->getDegree(param, indegreeNode->getVId(), indegreeNode->getEdgeType())
      .via(runner())
      .ensure([getIndegreeTime]() {
        VLOG(1) << "Get Indegree time: " << getIndegreeTime.elapsedInUSec() << "us";
      })
      .thenValue([this](StatusOr<GetDegreeResponse> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        auto value = std::move(resp).value();
        for (auto &code : value.get_result().get_failed_parts()) {
          NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
        }

        auto data = *value.props_ref();
        return finish(
            ResultBuilder().value(std::move(data)).iter(Iterator::Kind::kDefault).build());
      });
}

}  // namespace graph
}  // namespace nebula
