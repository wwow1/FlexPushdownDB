//
// Created by matt on 12/12/19.
//

#include "normal/pushdown/FileScan.h"

#include <cstdlib>                    // for abort
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>

#include <arrow/csv/options.h>         // for ReadOptions, ConvertOptions
#include <arrow/csv/reader.h>          // for TableReader
#include <arrow/io/file.h>             // for ReadableFile
#include <arrow/io/memory.h>           // for BufferReader
#include <arrow/result.h>              // for Result
#include <arrow/status.h>              // for Status
#include <arrow/type_fwd.h>            // for default_memory_pool

#include <normal/core/TupleMessage.h>
#include <normal/core/TupleSet.h>
#include <arrow/csv/parser.h>
#include <sstream>
#include <iostream>
#include <normal/core/CompleteMessage.h>

#include "normal/core/Message.h"       // for Message
#include "normal/core/Operator.h"      // for Operator
#include "io/CSVParser.h"

#include "normal/pushdown/Globals.h"

namespace arrow { class MemoryPool; }
namespace arrow::io { class InputStream; }

FileScan::FileScan(std::string name, std::string filePath)
    : Operator(std::move(name)), m_filePath(std::move(filePath)) {}

FileScan::~FileScan() = default;

void FileScan::onReceive(const normal::core::Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
    this->onStart();
  } else {
    Operator::onReceive(msg);
  }
}

void FileScan::onStart() {

  SPDLOG_DEBUG("Starting");

  arrow::Status st;
  auto pool = arrow::default_memory_pool();

  auto input = arrow::io::ReadableFile::Open(m_filePath).ValueOrDie();

  auto fields = CSVParser::readFields(input);

  st = input->Seek(0);
  if (!st.ok())
    abort();

  auto parseOptions = arrow::csv::ParseOptions::Defaults();
  auto readOptions = arrow::csv::ReadOptions::Defaults();
  readOptions.use_threads = false;
  auto convertOptions = arrow::csv::ConvertOptions::Defaults();

  auto reader = arrow::csv::TableReader::Make(pool,
                                              input,
                                              readOptions, parseOptions, convertOptions).ValueOrDie();

  auto tupleSet = TupleSet::make(reader);

  std::shared_ptr<normal::core::Message> message = std::make_shared<TupleMessage> (tupleSet);
  ctx()->tell(message);

  SPDLOG_DEBUG("Completing");
  std::shared_ptr<normal::core::Message> cm = std::make_shared<CompleteMessage> ();
  ctx()->tell(cm);

  ctx()->getOperatorActor()->quit();
}
