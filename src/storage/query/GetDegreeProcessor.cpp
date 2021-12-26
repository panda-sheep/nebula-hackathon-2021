/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/query/GetDegreeProcessor.h"

#include "common/utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kGetDegreeCounters;

void GetDegreeProcessor::process(const cpp2::GetDegreeRequest& req) {
  if (executor_ != nullptr) {
    executor_->add([req, this]() { this->doProcess(req); });
  } else {
    doProcess(req);
  }
}

void GetDegreeProcessor::doProcess(const cpp2::GetDegreeRequest& req) {
  spaceId_ = req.get_space_id();
  partId_ = req.get_part_id();
  vId_ = req.get_vertex_id().getStr();
  edgetype_ = req.get_edge_type();

  auto retCode = getSpaceVidLen(spaceId_);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    pushResultCode(retCode, partId_);
    onFinished();
    return;
  }

  if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId_)) {
    LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
               << " space vid len: " << spaceVidLen_ << ",  vid is " << vId_;
    pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_VID, partId_);
    onFinished();
    return;
  }

  auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgetype_));
  if (!schema) {
    LOG(ERROR) << "Space " << spaceId_ << ", Edge " << edgetype_ << " invalid";
    pushResultCode(nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND, partId_);
    onFinished();
    return;
  }

  // 1) First look for vertexRef to determine whether the vertex exists
  auto vertexRefRet = findVertexRef(partId_, vId_);
  if (!nebula::ok(vertexRefRet)) {
    retCode = nebula::error(vertexRefRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      LOG(ERROR) << "Space " << spaceId_ << ", vid is " << vId_ << " not exists.";
      retCode = nebula::cpp2::ErrorCode::E_VERTEX_NOT_FOUND;
    } else {
      LOG(ERROR) << "Space " << spaceId_ << ", get vertex ref failed,  vid is " << vId_;
    }
    pushResultCode(retCode, partId_);
    onFinished();
    return;
  }

  // 2）Count the out-degree or in-degree on edgetype of vId
  auto edgeRefPrefix = NebulaKeyUtils::edgeRefPrefix(spaceVidLen_, partId_, vId_, edgetype_);
  std::unique_ptr<kvstore::KVIterator> edgeIter;
  retCode = env_->kvstore_->prefix(spaceId_, partId_, edgeRefPrefix, &edgeIter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Get edge ref key failed " << apache::thrift::util::enumNameSafe(retCode);
    pushResultCode(retCode, partId_);
    onFinished();
    return;
  }

  degrees_ = 0;
  while (edgeIter && edgeIter->valid()) {
    degrees_ += NebulaKeyUtils::parseEdgeRefVal(edgeIter->val()).size();
    edgeIter->next();
  }
  onProcessFinished();
  onFinished();
  return;
}

ErrorOr<nebula::cpp2::ErrorCode, std::string> GetDegreeProcessor::findVertexRef(
    PartitionID partId, const VertexID& vId) {
  auto key = NebulaKeyUtils::vertexRefKey(spaceVidLen_, partId, vId);
  std::string val;
  auto ret = env_->kvstore_->get(spaceId_, partId, key, &val);
  if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
    return val;
  } else {
    LOG(ERROR) << "Error! ret = " << apache::thrift::util::enumNameSafe(ret) << ", spaceId "
               << spaceId_;
    return ret;
  }
}

void GetDegreeProcessor::onProcessFinished() {
  if (edgetype_ > 0) {
    resultDataSet_.colNames.emplace_back("outdegree");
  } else {
    resultDataSet_.colNames.emplace_back("indegree");
  }
  std::vector<Value> row;
  row.emplace_back(degrees_);
  resultDataSet_.rows.emplace_back(std::move(row));
  resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
