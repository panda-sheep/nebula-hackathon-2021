/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/FetchIndegreeValidator.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/ValidateUtil.h"

namespace nebula {
namespace graph {

Status FetchIndegreeValidator::validateImpl() {
  auto *sentence = static_cast<FetchIndegreeSentence *>(sentence_);
  edgeName_ = *sentence->getEdgeName();
  if (edgeName_.empty()) {
    return Status::SemanticError("Fetch indegree not allow vid or edgename empty.");
  }

  auto ret = SchemaUtil::toVertexID(sentence->getVid(), vidType_);
  if (!ret.ok()) {
    LOG(ERROR) << ret.status();
    return ret.status();
  }
  vId_ = std::move(ret).value();
  NG_RETURN_IF_ERROR(validateEdgeName());
  return Status::OK();
}

Status FetchIndegreeValidator::validateEdgeName() {
  spaceId_ = space_.id;
  auto status = qctx_->schemaMng()->toEdgeType(spaceId_, edgeName_);
  NG_RETURN_IF_ERROR(status);
  edgeType_ = -status.value();
  auto edgeSchema = qctx_->schemaMng()->getEdgeSchema(spaceId_, edgeType_);
  if (edgeSchema == nullptr) {
    return Status::SemanticError("No schema found for `%s'", edgeName_.c_str());
  }
  return Status::OK();
}

Status FetchIndegreeValidator::toPlan() {
  auto *node = GetIndegree::make(qctx_, nullptr, spaceId_, vId_, edgeType_);
  root_ = node;
  tail_ = root_;
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
