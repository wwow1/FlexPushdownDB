//
// Created by matt on 12/12/19.
//

#include "normal/tuple/TupleSet.h"
#include "normal/tuple/Globals.h"

#include <utility>
#include <sstream>
#include <cassert>
#include <cstdlib>                      // for abort
#include <memory>                        // for __shared_ptr_access, __share...

#include <arrow/api.h>                 // for Array, NumericArray, StringA...
#include <arrow/csv/api.h>            // for TableReader
#include <tl/expected.hpp>
#include <arrow/scalar.h>

namespace arrow { class MemoryPool; }

using namespace normal::tuple;

std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::csv::TableReader> &tableReader) {

  auto result = tableReader->Read();
  if (!result.ok()) {
    // FIXME
    abort();
  }

  auto tupleSet = std::make_shared<TupleSet>();
  auto table = result.ValueOrDie();
  tupleSet->table_ = table;

  assert(tupleSet);
  assert(tupleSet->table_);
  assert(tupleSet->table_->ValidateFull().ok());

  return tupleSet;
}

std::shared_ptr<TupleSet> TupleSet::make(std::shared_ptr<arrow::Table> table) {

  auto tupleSet = std::make_shared<TupleSet>();
  tupleSet->table_ = std::move(table);

  return tupleSet;
}

std::shared_ptr<arrow::Table> TupleSet::table() const {
  return table_;
}

void TupleSet::table(const std::shared_ptr<arrow::Table> &table) {
  table_ = table;
}

std::shared_ptr<TupleSet> TupleSet::concatenate(const std::shared_ptr<TupleSet> &tp1,
                                                const std::shared_ptr<TupleSet> &tp2) {
  std::shared_ptr<arrow::Table> tb1 = tp1->table_;
  std::shared_ptr<arrow::Table> tb2 = tp2->table_;
  std::vector<std::shared_ptr<arrow::Table>> tblVector = {tb1, tb2};

  auto res = arrow::ConcatenateTables(tblVector);
  if (!res.ok())
    abort();
  auto resTupleSet = make(*res);
  return resTupleSet;
}
void TupleSet::addColumn(const std::string &name, int position, std::vector<std::string> data) {
  arrow::Status arrowStatus;

  arrow::MemoryPool *pool = arrow::default_memory_pool();
  arrow::StringBuilder colBuilder(pool);

  for (int64_t r = 0; r < table_->num_rows(); ++r) {
    std::string s = data.at(r);
    arrowStatus = colBuilder.Append(s.c_str()); // FIXME: Not sure if this is safe

    if (!arrowStatus.ok())
      abort();
  }

  std::shared_ptr<arrow::StringArray> col;
  arrowStatus = colBuilder.Finish(&col);

  if (!arrowStatus.ok())
    abort();

  auto chunked_col = std::make_shared<arrow::ChunkedArray>(col);

  std::shared_ptr<arrow::Field> field;
  field = arrow::field(name, arrow::utf8());

  arrowStatus = table_->AddColumn(position, field, chunked_col, &table_);

  if (!arrowStatus.ok())
    abort();
}

int64_t TupleSet::numRows() {
  return table_->num_rows();
}

int64_t TupleSet::numColumns() {
  return table_->num_columns();
}

std::shared_ptr<arrow::Scalar> TupleSet::visit(const std::function<std::shared_ptr<arrow::Scalar>(std::shared_ptr<arrow::Scalar>, arrow::RecordBatch &)> &fn) {

  arrow::Status arrowStatus;

  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::TableBatchReader reader(*table_);
  reader.set_chunksize(tuple::DefaultChunkSize);
  arrowStatus = reader.ReadNext(&batch);

  std::shared_ptr<arrow::Scalar> result;
  while (arrowStatus.ok() && batch) {
    result = fn(result, *batch);
    arrowStatus = reader.ReadNext(&batch);
  }

  return result;
}

/**
 *
 * @return
 */
std::string TupleSet::toString() {

  auto ss = std::stringstream();
  arrow::Status arrowStatus = arrow::PrettyPrint(*table_, 0, &ss);

  if (!arrowStatus.ok()) {
    // FIXME
    abort();
  }

  return ss.str();
}

std::string TupleSet::getValue(const std::string &columnName, int row) {

  assert(row >= 0);

  auto chunkedArray = table_->GetColumnByName(columnName);

  // FIXME: Only support strings at the moment
  assert(chunkedArray->type()->id() == arrow::Type::type::STRING);

  // TODO: Not sure if this is the best way to access a particular row
  auto slicedArray = chunkedArray->Slice(row, row + 1);
  auto array = std::static_pointer_cast<arrow::StringArray>(slicedArray->chunk(0));
  auto value = array->GetString(row);

  return value;
}

//tl::expected<std::shared_ptr<TupleSet>, std::string>
//TupleSet::evaluate(const std::shared_ptr<normal::expression::Projector> &projector) {
//
//  // Read the table in batches
//  std::shared_ptr<arrow::RecordBatch> batch;
//  arrow::TableBatchReader reader(*table_);
//  reader.set_chunksize(DEFAULT_CHUNK_SIZE);
//  auto res = reader.ReadNext(&batch);
//  std::shared_ptr<TupleSet> resultTuples = nullptr;
//  while (res.ok() && batch) {
//
//	// Evaluate expressions against a batch
//	std::shared_ptr<arrow::ArrayVector> outputs = projector->evaluate(*batch);
//	auto batchResultTuples = normal::core::TupleSet::make(projector->getResultSchema(), *outputs);
//
//	// Concatenate the batch result to the full results
//	if (resultTuples)
//	  resultTuples = concatenate(batchResultTuples, resultTuples);
//	else
//	  resultTuples = batchResultTuples;
//
//	res = reader.ReadNext(&batch);
//  }
//
//  return resultTuples;
//
//}