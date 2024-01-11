//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEKERNEL_H

#include <string>
#include <memory>

#include <normal/tuple/TupleSet2.h>
#include "SlicedBloomFilter.h"

using namespace normal::tuple;
class BloomCreateKernel {

public:
  BloomCreateKernel(std::string columnName,
					double desiredFalsePositiveRate,
					std::vector<std::string> bloomJoinUseSqlTemplates);
  static std::shared_ptr<BloomCreateKernel> make(const std::string &columnName,
												 double desiredFalsePositiveRate,
												 const std::vector<std::string> &bloomJoinUseSqlTemplates);

  [[nodiscard]] tl::expected<void, std::string> addTupleSet(const std::shared_ptr<TupleSet2> &tupleSet);
  [[nodiscard]] tl::expected<void, std::string> buildBloomFilter();
  [[nodiscard]] const std::optional<std::shared_ptr<SlicedBloomFilter>> &getBloomFilter() const;

private:
  inline static constexpr int MaxS3SelectExpressionLength = 1024 * 256;
  inline static constexpr int MaxBloomFilterPredicateSQLTemplateLength = 1196;

  std::string columnName_;
  double desiredFalsePositiveRate_;
  std::vector<std::string> bloomJoinUseSQLTemplates_;

  std::optional<std::shared_ptr<TupleSet2>> receivedTupleSet_;
  std::optional<std::shared_ptr<SlicedBloomFilter>> bloomFilter_;

	template<typename ArrowArrayType>
  void addRecordBatchToBloomFilter(::arrow::RecordBatch &recordBatch, int keyColumnIndex);
	
	[[nodiscard]] tl::expected<void, std::string> addRecordBatchToBloomFilter(::arrow::RecordBatch &recordBatch, int keyColumnIndex);
  tl::expected<double, std::string> calculateBestFalsePositiveRate(const size_t &maxBloomJoinUseSQLTemplateSize);
};

  template<typename ArrowArrayType>
  void BloomCreateKernel::addRecordBatchToBloomFilter(::arrow::RecordBatch &recordBatch, int keyColumnIndex) {
	auto keyColumn = std::static_pointer_cast<ArrowArrayType>(recordBatch.column(keyColumnIndex));
	for (int r = 0; r < recordBatch.num_rows(); ++r) {
	  bloomFilter_.value()->add(keyColumn->Value(r));
	}
  }

  template<>
  void BloomCreateKernel::addRecordBatchToBloomFilter<::arrow::StringArray>(::arrow::RecordBatch &recordBatch, int keyColumnIndex) {
		auto keyColumn = std::static_pointer_cast<::arrow::StringArray>(recordBatch.column(keyColumnIndex));
		for (int r = 0; r < recordBatch.num_rows(); ++r) {
			bloomFilter_.value()->add(std::stoi(keyColumn->GetString(r)));
		}
	}
#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEKERNEL_H
