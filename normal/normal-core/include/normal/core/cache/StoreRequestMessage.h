//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H

#include <caf/all.hpp>

#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>

#include <normal/core/message/Message.h>

using namespace caf;
using namespace normal::cache;
using namespace normal::core::message;

namespace normal::core::cache {

/**
 * Reuest for the segment cache actor to store the given segment data given a segment key.
 */
class StoreRequestMessage: public Message {

public:
  StoreRequestMessage(std::shared_ptr<SegmentKey> SegmentKey,
					  std::shared_ptr<SegmentData> SegmentData,
					  const std::string &sender);

  static std::shared_ptr<StoreRequestMessage> make(std::shared_ptr<SegmentKey> SegmentKey,
												   std::shared_ptr<SegmentData> SegmentData,
												   const std::string &sender);

  [[nodiscard]] const std::shared_ptr<SegmentKey> &getSegmentKey() const;
  [[nodiscard]] const std::shared_ptr<SegmentData> &getSegmentData() const;

  [[nodiscard]] std::string toString() const;

private:
  std::shared_ptr<SegmentKey> segmentKey_;
  std::shared_ptr<SegmentData> segmentData_;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::cache::StoreRequestMessage)

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H