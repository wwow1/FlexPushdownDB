//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEKERNEL_H

#include <utility>

#include <normal/tuple/csv/CSVParser.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/pushdown/bloomjoin/SlicedBloomFilter.h>
#include <normal/tuple/ArrayAppenderWrapper.h>

using namespace normal::tuple;
using namespace normal::tuple::csv;

class FileScanBloomUseKernel {

public:
  FileScanBloomUseKernel(std::string filePath,
						 std::vector<std::string> columnNames,
						 unsigned long startOffset,
						 unsigned long finishOffset,
						 std::string bloomFilterColumnName) :
	  filePath_(std::move(filePath)),
	  columnNames_(std::move(columnNames)),
	  startOffset_(startOffset),
	  finishOffset_(finishOffset),
	  bloomFilterColumnName_(std::move(bloomFilterColumnName)) {
  }

  static std::shared_ptr<FileScanBloomUseKernel> make(const std::string &filePath,
													  const std::vector<std::string> &columnNames,
													  unsigned long startOffset,
													  unsigned long finishOffset,
													  const std::string &bloomFilterColumnName) {
	return std::make_shared<FileScanBloomUseKernel>(filePath,
													columnNames,
													startOffset,
													finishOffset,
													bloomFilterColumnName);
  }

  [[nodiscard]]  tl::expected<void, std::string> setBloomFilter(const std::shared_ptr<SlicedBloomFilter> &bloomFilter) {

	if (bloomFilter_)
	  return tl::make_unexpected("Bloom filter already set");

	bloomFilter_ = bloomFilter;
	return {};
  }

  [[nodiscard]] tl::expected<void, std::string> scan(const std::vector<std::string> &columnNames) {

	CSVParser parser_(filePath_, columnNames, startOffset_, finishOffset_);

	auto expectedTupleSet = parser_.parse();
	if (!expectedTupleSet)
	  return tl::make_unexpected(expectedTupleSet.error());

	tupleSet_ = expectedTupleSet.value();

	return {};
  }

  size_t size() {
	if (!tupleSet_)
	  return 0;
	else
	  return tupleSet_.value()->numRows();
  }

  template<typename ArrowArrayType>
  void filterRecordBatch(const ::arrow::RecordBatch &recordBatch,
						 int keyColumnIndex,
						 const std::vector<std::shared_ptr<ArrayAppender>>& appenders) {
	std::vector<std::shared_ptr<::arrow::Array>> columns(recordBatch.num_columns());
	for (int c = 0; c < recordBatch.num_columns(); ++c) {
	  columns[c] = recordBatch.column(c);
	}

	auto columnArray = std::static_pointer_cast<ArrowArrayType>(recordBatch.column(keyColumnIndex));

	for (int r = 0; r < recordBatch.num_rows(); ++r) {
	  if (bloomFilter_.value()->contains(columnArray->Value(r))) {
		for (size_t c = 0; c < appenders.size(); ++c) {
		  appenders[c]->appendValue(columns[c], r);
		}
	  }
	}
  }

  [[nodiscard]] tl::expected<void, std::string> filterRecordBatch(const ::arrow::RecordBatch &recordBatch,
																  int keyColumnIndex,
																  const std::vector<std::shared_ptr<ArrayAppender>>& appenders);

  [[nodiscard]] tl::expected<void, std::string> filter();

  [[nodiscard]] const std::optional<std::shared_ptr<TupleSet2>> &getTupleSet() const {
	return tupleSet_;
  }

private:
  std::string filePath_;
  std::vector<std::string> columnNames_;
  unsigned long startOffset_;
  unsigned long finishOffset_;
  std::string bloomFilterColumnName_;

  std::optional<std::shared_ptr<SlicedBloomFilter>> bloomFilter_;

  std::optional<std::shared_ptr<TupleSet2>> tupleSet_;

};

  template<>
  void FileScanBloomUseKernel::filterRecordBatch<::arrow::StringArray>(const ::arrow::RecordBatch &recordBatch,
											   int keyColumnIndex,
											   const std::vector<std::shared_ptr<ArrayAppender>>& appenders) {
	std::vector<std::shared_ptr<::arrow::Array>> columns(recordBatch.num_columns());
	for (int c = 0; c < recordBatch.num_columns(); ++c) {
	  columns[c] = recordBatch.column(c);
	}

	auto columnArray = std::static_pointer_cast<::arrow::StringArray>(recordBatch.column(keyColumnIndex));

	for (int r = 0; r < recordBatch.num_rows(); ++r) {
	  if (bloomFilter_.value()->contains(std::stoi(columnArray->GetString(r)))) {
		for (size_t c = 0; c < appenders.size(); ++c) {
		  appenders[c]->appendValue(columns[c], r);
		}
	  }
	}
  }

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEKERNEL_H
