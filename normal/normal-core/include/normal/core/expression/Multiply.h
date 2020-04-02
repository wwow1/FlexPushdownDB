//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_MULTIPLY_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_MULTIPLY_H

#include "Expression.h"

#include <memory>

namespace normal::core::expression {

template<class T>
class Multiply : public normal::core::expression::Expression<T> {
private:
  std::unique_ptr<normal::core::expression::Expression<T>> left_;
  std::unique_ptr<normal::core::expression::Expression<T>> right_;

public:
  Multiply(std::unique_ptr<normal::core::expression::Expression<T>> left,
           std::unique_ptr<normal::core::expression::Expression<T>> right)
      : left_(std::move(left)), right_(std::move(right)) {}

  T eval() override {
    return left_->eval() * right_->eval();
  }

};

template<class T>
static std::unique_ptr<normal::core::expression::Expression<T>>
times(std::unique_ptr<normal::core::expression::Expression<T>> left,
      std::unique_ptr<normal::core::expression::Expression<T>> right) {
  return std::make_unique<Multiply<T>>(std::move(left), std::move(right));
}

}

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_MULTIPLY_H
