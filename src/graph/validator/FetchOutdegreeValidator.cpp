/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/FetchOutdegreeValidator.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/ValidateUtil.h"

namespace nebula {
namespace graph {

Status FetchOutdegreeValidator::validateImpl() {
  auto *sentence = static_cast<FetchOutdegreeSentence *>(sentence_);
  edgeName_ = *sentence->getEdgeName();
  if (edgeName_.empty()) {
    return Status::SemanticError("Fetch outdegree not allow vid or edgename empty.");
  }
  auto idRet = SchemaUtil::toVertexID(sentence->getVid(), vidType_);
  if (!idRet.ok()) {
    LOG(ERROR) << idRet.status();
    return idRet.status();
  }
  vId_ = std::move(idRet).value();
  NG_RETURN_IF_ERROR(validateEdgeName());
  return Status::OK();
}

Status FetchOutdegreeValidator::validateEdgeName() {
  spaceId_ = space_.id;
  auto status = qctx_->schemaMng()->toEdgeType(spaceId_, edgeName_);
  NG_RETURN_IF_ERROR(status);
  edgeType_ = status.value();
  auto edgeSchema = qctx_->schemaMng()->getEdgeSchema(spaceId_, edgeType_);
  if (edgeSchema == nullptr) {
    return Status::SemanticError("No schema found for `%s'", edgeName_.c_str());
  }
  return Status::OK();
}

Status FetchOutdegreeValidator::toPlan() {
  auto *node = GetOutdegree::make(qctx_, nullptr, spaceId_, vId_, edgeType_);
  root_ = node;
  tail_ = root_;
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
