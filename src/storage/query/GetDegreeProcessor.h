/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_GETDEGREEPROCESSOR_H_
#define STORAGE_QUERY_GETDEGREEPROCESSOR_H_

#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kGetDegreeCounters;

class GetDegreeProcessor
    : public QueryBaseProcessor<cpp2::GetDegreeRequest, cpp2::GetDegreeResponse> {
 public:
  static GetDegreeProcessor* instance(StorageEnv* env,
                                      const ProcessorCounters* counters = &kGetDegreeCounters,
                                      folly::Executor* executor = nullptr) {
    return new GetDegreeProcessor(env, counters, executor);
  }

  void process(const cpp2::GetDegreeRequest& req) override;

  void doProcess(const cpp2::GetDegreeRequest& req);

 private:
  GetDegreeProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor)
      : QueryBaseProcessor<cpp2::GetDegreeRequest, cpp2::GetDegreeResponse>(
            env, counters, executor) {}

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetDegreeRequest&) override {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  };

  ErrorOr<nebula::cpp2::ErrorCode, std::string> findVertexRef(PartitionID partId,
                                                              const VertexID& vId);

  void onProcessFinished() override;

 private:
  VertexID vId_;
  PartitionID partId_;
  EdgeType edgetype_;
  int64_t degrees_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETDEGREEPROCESSOR_H_
