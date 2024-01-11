//
// Created by matt on 5/8/20.
//

#include "normal/pushdown/bloomjoin/FileScanBloomUseKernel.h"

	[[nodiscard]] tl::expected<void, std::string> FileScanBloomUseKernel::filter() {

	::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
	::arrow::Status status;

	auto table = tupleSet_.value()->getArrowTable().value();
	auto filterColumnIndex = table->schema()->GetFieldIndex(bloomFilterColumnName_);

	std::vector<std::shared_ptr<ArrayAppender>> appenders(table->num_columns());
	for (int c = 0; c < table->num_columns(); ++c) {
	  auto expectedAppender = ArrayAppenderBuilder::make(table->column(c)->type(), 0);
	  if (!expectedAppender.has_value())
		return tl::make_unexpected(expectedAppender.error());
	  appenders[c] = expectedAppender.value();
	}

	::arrow::TableBatchReader reader(*table);
	reader.set_chunksize(DefaultChunkSize);

	// Read a batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok()) {
	  return tl::make_unexpected(recordBatchResult.status().message());
	}
	auto recordBatch = *recordBatchResult;

	while (recordBatch) {

	  auto filterResult = filterRecordBatch(*recordBatch, filterColumnIndex, appenders);
	  if (!filterResult)
		return tl::make_unexpected(filterResult.error());

	  // Read a batch
	  recordBatchResult = reader.Next();
	  if (!recordBatchResult.ok()) {
		return tl::make_unexpected(recordBatchResult.status().message());
	  }
	  recordBatch = *recordBatchResult;
	}

	::arrow::ArrayVector filteredArrayVector_(table->schema()->num_fields());

	for (size_t c = 0; c < appenders.size(); ++c) {
	  auto expectedArray = appenders[c]->finalize();
	  if (!expectedArray.has_value())
		return tl::make_unexpected(expectedArray.error());
	  filteredArrayVector_[c] = expectedArray.value();
	}

	auto filteredTable = ::arrow::Table::Make(table->schema(), filteredArrayVector_);
	tupleSet_ = TupleSet2::make(filteredTable);

	return {};
  }

  [[nodiscard]] tl::expected<void, std::string> FileScanBloomUseKernel::filterRecordBatch(const ::arrow::RecordBatch &recordBatch,
                                int keyColumnIndex,
                                const std::vector<std::shared_ptr<ArrayAppender>>& appenders) {
	auto columnTypeId = recordBatch.column(keyColumnIndex)->type_id();

	switch (columnTypeId) {
	case arrow::Type::BOOL: filterRecordBatch<::arrow::BooleanArray>(recordBatch, keyColumnIndex, appenders);
	  break;
	case arrow::Type::INT8: filterRecordBatch<::arrow::Int8Array>(recordBatch, keyColumnIndex, appenders);
	  break;
	case arrow::Type::INT16: filterRecordBatch<::arrow::Int16Array>(recordBatch, keyColumnIndex, appenders);
	  break;
	case arrow::Type::INT32: filterRecordBatch<::arrow::Int32Array>(recordBatch, keyColumnIndex, appenders);
	  break;
	case arrow::Type::INT64: filterRecordBatch<::arrow::Int64Array>(recordBatch, keyColumnIndex, appenders);
	  break;
	case arrow::Type::STRING: filterRecordBatch<::arrow::StringArray>(recordBatch, keyColumnIndex, appenders);
	  break;
	default:
	  return tl::make_unexpected(fmt::format(
		  "Filter is not implemented for arrays of type {}",
		  columnTypeId));
	}

	return {};
  }