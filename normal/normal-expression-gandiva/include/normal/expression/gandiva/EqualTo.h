//
// Created by matt on 11/6/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EQUALTO_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EQUALTO_H


#include <string>
#include <memory>

#include "Expression.h"

namespace normal::expression::gandiva {

class EqualTo : public Expression {

public:
  EqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

private:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;

};

std::shared_ptr<Expression> eq(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EQUALTO_H
