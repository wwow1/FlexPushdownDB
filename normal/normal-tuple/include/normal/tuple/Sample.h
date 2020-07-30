//
// Created by matt on 22/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H

#include <memory>

#include "TupleSet2.h"

namespace normal::tuple {

/**
 * Pre built sample tuple sets, useful for testing
 */
class Sample {

public:

  /**
   * 3 x 3 tuple set of strings
   *
   * @return
   */
  static std::shared_ptr<TupleSet2> sample3x3String();

  static std::shared_ptr<Column> sample3String();

  /**
   * Creates a  numCols x numRows tuple set of random decimal strings between 0.0 and 100.0
   *
   * Each column is named "c_<column index>"
   *
   * @param numCols
   * @param numRows
   * @return
   */
  static std::shared_ptr<TupleSet2> sampleCxRString(int numCols, int numRows);
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H
