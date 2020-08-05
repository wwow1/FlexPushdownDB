//
// Created by Yifei Yang on 8/3/20.
//

#include "normal/cache/FBRCachingPolicy.h"

using namespace normal::cache;

bool lessFrequent (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  return key1->getMetadata()->hitNum() < key2->getMetadata()->hitNum();
}

FBRCachingPolicy::FBRCachingPolicy(size_t maxSize) :
        CachingPolicy(maxSize) {}

std::shared_ptr<FBRCachingPolicy> FBRCachingPolicy::make(size_t maxSize) {
  return std::make_shared<FBRCachingPolicy>(maxSize);
}

void FBRCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  auto keyEntry = keySet_.find(key);
  if (keyEntry != keySet_.end()) {
    keyEntry->get()->getMetadata()->incHitNum();
  }
}

void FBRCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
FBRCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  // decide whether to cache
  std::shared_ptr<SegmentKey> realKey;
  auto keyEntry = keySet_.find(key);
  if (keyEntry != keySet_.end()) {
    realKey = *keyEntry;
    if (realKey->getMetadata()->size() == 0) {
      realKey->getMetadata()->setSize(key->getMetadata()->size());
    }
  } else {
    throw std::runtime_error("Key should exist in keySet_");
  }

  auto segmentSize = realKey->getMetadata()->size();
  if (maxSize_ < segmentSize) {
    removeEstimateCachingDecision(realKey);
    return std::nullopt;
  }

  std::sort(keysInCache_.begin(), keysInCache_.end(), lessFrequent);
  int heapIndex = 0;
  size_t tmpFreeSize = freeSize_;
  while (tmpFreeSize < segmentSize) {
    auto removableKey = keysInCache_[heapIndex];
    if (removableKey->getMetadata()->hitNum() < realKey->getMetadata()->hitNum()) {
      removableKeys->emplace_back(removableKey);
      tmpFreeSize += removableKey->getMetadata()->size();
      ++heapIndex;
    } else {
      removeEstimateCachingDecision(realKey);
      return std::nullopt;
    }
  }

  // remove
  if (heapIndex > 0) {
    keysInCache_.erase(keysInCache_.begin(), keysInCache_.begin() + heapIndex);
    freeSize_ = tmpFreeSize;
  }

  // bring in
  keysInCache_.emplace_back(realKey);
  freeSize_ -= segmentSize;

  removeEstimateCachingDecision(realKey);
  return std::optional(removableKeys);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
FBRCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  auto keysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  // FIXME: an estimation here, if freeSize_ >= c * maxSize_, we try to cache all segments
  //  Because we cannot know the size of segmentData before bringing it back
  if (freeSize_ >= maxSize_ * 0.1) {
    for (const auto &key: *segmentKeys) {
      auto keyEntry = keySet_.find(key);
      if (keyEntry != keySet_.end()) {
        (*keyEntry)->getMetadata()->incHitNum();
      } else {
        keySet_.emplace(key);
      }
    }
    return segmentKeys;
  }

  for (auto key: *segmentKeys) {
    // estimate whether to cache
    std::shared_ptr<SegmentKey> realKey;
    auto keyEntry = keySet_.find(key);
    if (keyEntry != keySet_.end()) {
      realKey = *keyEntry;
      realKey->getMetadata()->incHitNum();
    } else {
      realKey = key;
      keySet_.emplace(realKey);
    }

    // try to find one lower-value unused key in cache
    for (const auto &keyInCache: keysInCache_) {
      if (keyInCache->getMetadata()->hitNum() < realKey->getMetadata()->hitNum() &&
        keysToReplace_.find(keyInCache) == keysToReplace_.end()) {
        keysToCache->emplace_back(realKey);
        addEstimateCachingDecision(realKey, keyInCache);
        break;
      }
    }
  }

  return keysToCache;
}

void FBRCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  keysInCache_.erase(std::find(keysInCache_.begin(), keysInCache_.end(), key));
}

long FBRCachingPolicy::value(std::shared_ptr<SegmentMetadata> metadata) {
  return metadata->hitNum() / metadata->size();
}

void FBRCachingPolicy::addEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in,
                                                  const std::shared_ptr<SegmentKey> &out) {
  keysToReplace_.emplace(out);
  estimateCachingDecisions_.emplace(in, out);
}

void FBRCachingPolicy::removeEstimateCachingDecision(const std::shared_ptr<SegmentKey> &in) {
  auto keysToReplaceEntry = estimateCachingDecisions_.find(in);
  if (keysToReplaceEntry != estimateCachingDecisions_.end()) {
    keysToReplace_.erase(keysToReplaceEntry->second);
    estimateCachingDecisions_.erase(in);
  }
}
