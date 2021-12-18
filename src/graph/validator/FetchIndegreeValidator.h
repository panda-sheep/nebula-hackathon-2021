/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef _VALIDATOR_FETCH_INDEGREE_VALIDATOR_H_
#define _VALIDATOR_FETCH_INDEGREE_VALIDATOR_H_

#include "graph/context/ast/QueryAstContext.h"
#include "graph/validator/Validator.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class FetchIndegreeValidator final : public Validator {
 public:
  FetchIndegreeValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status validateEdgeName();

  Status toPlan() override;

 private:
  GraphSpaceID spaceId_{-1};
  Value vId_;

  std::string edgeName_;
  EdgeType edgeType_{-1};
};

}  // namespace graph
}  // namespace nebula

#endif  // _VALIDATOR_FETCH_INDEGREE_VALIDATOR_H_
