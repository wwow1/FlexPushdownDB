//
// Created by matt on 11/6/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_GREATERTHANOREQUALTO_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_GREATERTHANOREQUALTO_H


#include <string>
#include <memory>

#include "Expression.h"
#include "BinaryExpression.h"

namespace normal::expression::gandiva {

class GreaterThanOrEqualTo : public BinaryExpression {

public:
  GreaterThanOrEqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

};

std::shared_ptr<Expression> gte(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_GREATERTHANOREQUALTO_H
