#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../util.h"
#include "base.h"
#include "lipp/src/core/lipp.h"
#include "pgm_index_dynamic.hpp"

// HybridPGMLIPP v3: double-buffered hash-map write buffer + LIPP main store.
//
// Architecture:
//   - LIPP main store holds the bulk of keys (bulk-loaded initially).
//   - An "active" hash map absorbs new inserts with O(1) insert and lookup.
//   - A "frozen" hash map is being incrementally drained into LIPP.
//   - Hash map find-miss is a single bucket access (~15-25ns), much cheaper
//     than a Bloom filter (3 L3 accesses) or DynamicPGM search (multi-level
//     traversal), so no Bloom filter is needed.
//
// On Insert: key goes into the active hash map + shadow vector, then a small
//   batch of frozen keys is drained into LIPP (amortised flush). When the
//   active buffer reaches the threshold, buffers are swapped.
//
// On Lookup: active map → frozen map (if present) → LIPP.
template <class KeyType, class SearchClass, size_t pgm_error,
          size_t flush_threshold>
class HybridPGMLIPP : public Base<KeyType> {
  using MapType = std::unordered_map<KeyType, uint64_t>;
  using KVPair = std::pair<KeyType, uint64_t>;

 public:
  HybridPGMLIPP(const std::vector<int>& /*params*/) {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t /*num_threads*/) {
    std::vector<KVPair> pairs;
    pairs.reserve(data.size());
    for (const auto& kv : data) pairs.emplace_back(kv.key, kv.value);

    uint64_t t = util::timing(
        [&] { lipp_.bulk_load(pairs.data(), (int)pairs.size()); });

    total_keys_ = data.size();
    active_count_ = 0;
    has_frozen_ = false;
    frozen_cursor_ = 0;

    size_t expected_buf = total_keys_ / flush_threshold + 1;
    active_map_.reserve(expected_buf);

    return t;
  }

  size_t EqualityLookup(const KeyType& key, uint32_t /*tid*/) const {
    auto it = active_map_.find(key);
    if (it != active_map_.end()) return it->second;

    if (has_frozen_) {
      auto it2 = frozen_map_.find(key);
      if (it2 != frozen_map_.end()) return it2->second;
    }

    uint64_t value;
    if (!lipp_.find(key, value)) return util::NOT_FOUND;
    return value;
  }

  uint64_t RangeQuery(const KeyType& /*lower*/, const KeyType& /*upper*/,
                      uint32_t /*tid*/) const {
    return 0;
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t /*tid*/) {
    active_map_.emplace(kv.key, kv.value);
    active_shadow_.emplace_back(kv.key, kv.value);
    ++active_count_;
    ++total_keys_;

    DrainBatch();

    if (active_count_ * flush_threshold >= total_keys_) SwapBuffers();
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const {
    size_t s = lipp_.index_size();
    s += active_map_.bucket_count() * sizeof(void*) +
         active_map_.size() * (sizeof(KeyType) + sizeof(uint64_t) + sizeof(void*));
    if (has_frozen_) {
      s += frozen_map_.bucket_count() * sizeof(void*) +
           frozen_map_.size() * (sizeof(KeyType) + sizeof(uint64_t) + sizeof(void*));
    }
    return s;
  }

  bool applicable(bool unique, bool /*range_query*/, bool /*insert*/,
                  bool multithread,
                  const std::string& /*ops_filename*/) const {
    std::string sname = SearchClass::name();
    return unique && !multithread && sname != "LinearAVX";
  }

  std::vector<std::string> variants() const {
    return {SearchClass::name(), std::to_string(pgm_error),
            std::to_string(flush_threshold)};
  }

 private:
  static constexpr size_t kDrainBatch = 8;

  void DrainBatch() {
    if (!has_frozen_) return;
    size_t end =
        std::min(frozen_cursor_ + kDrainBatch, frozen_shadow_.size());
    while (frozen_cursor_ < end) {
      const auto& kv = frozen_shadow_[frozen_cursor_];
      lipp_.insert(kv.first, kv.second);
      ++frozen_cursor_;
    }
    if (frozen_cursor_ >= frozen_shadow_.size()) FinishFrozen();
  }

  void DrainAll() {
    if (!has_frozen_) return;
    while (frozen_cursor_ < frozen_shadow_.size()) {
      const auto& kv = frozen_shadow_[frozen_cursor_];
      lipp_.insert(kv.first, kv.second);
      ++frozen_cursor_;
    }
    FinishFrozen();
  }

  void FinishFrozen() {
    frozen_map_.clear();
    frozen_shadow_.clear();
    frozen_cursor_ = 0;
    has_frozen_ = false;
  }

  void SwapBuffers() {
    if (has_frozen_) DrainAll();

    frozen_map_ = std::move(active_map_);
    frozen_shadow_ = std::move(active_shadow_);
    frozen_cursor_ = 0;
    has_frozen_ = true;

    size_t expected_buf = total_keys_ / flush_threshold + 1;
    active_map_ = MapType();
    active_map_.reserve(expected_buf);
    active_shadow_.clear();
    active_shadow_.reserve(expected_buf);
    active_count_ = 0;
  }

  // Main store
  LIPP<KeyType, uint64_t> lipp_;

  // Active buffer (absorbs new inserts)
  MapType active_map_;
  std::vector<KVPair> active_shadow_;
  size_t active_count_ = 0;

  // Frozen buffer (being incrementally drained into LIPP)
  MapType frozen_map_;
  std::vector<KVPair> frozen_shadow_;
  size_t frozen_cursor_ = 0;
  bool has_frozen_ = false;

  size_t total_keys_ = 0;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
