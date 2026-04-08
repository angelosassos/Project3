#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <string>
#include <vector>

#include "../util.h"
#include "base.h"
#include "lipp/src/core/lipp.h"
#include "pgm_index_dynamic.hpp"

// HybridPGMLIPP: uses DynamicPGM as a write buffer and LIPP as the
// read-optimized main store. On Build, all data is bulk-loaded into LIPP.
// Inserts go into DPGM. When DPGM accumulates >= (1/flush_threshold) of total
// keys, all keys are flushed from DPGM into LIPP one-by-one, and DPGM is reset.
// Lookups check DPGM first (it is small), then fall through to LIPP.
//
// Note: A shadow buffer mirrors DPGM's contents so we can iterate during flush,
// because the DynamicPGMIndex::begin() iterator has an API mismatch in this
// codebase. The shadow buffer is reset together with DPGM on every flush.
template <class KeyType, class SearchClass, size_t pgm_error,
          size_t flush_threshold>
class HybridPGMLIPP : public Base<KeyType> {
  using DPGMType =
      DynamicPGMIndex<KeyType, uint64_t, SearchClass,
                      PGMIndex<KeyType, SearchClass, pgm_error, 16>>;

 public:
  HybridPGMLIPP(const std::vector<int>& /*params*/) {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t /*num_threads*/) {
    std::vector<std::pair<KeyType, uint64_t>> pairs;
    pairs.reserve(data.size());
    for (const auto& kv : data)
      pairs.emplace_back(kv.key, kv.value);

    uint64_t t = util::timing(
        [&] { lipp_.bulk_load(pairs.data(), (int)pairs.size()); });

    total_keys_ = data.size();
    dpgm_count_ = 0;
    return t;
  }

  size_t EqualityLookup(const KeyType& key, uint32_t /*tid*/) const {
    // Check the (small) DPGM buffer first.
    auto it = dpgm_.find(key);
    if (it != dpgm_.end()) return it->value();

    // Fall back to LIPP main store.
    uint64_t value;
    if (!lipp_.find(key, value)) return util::NOT_FOUND;
    return value;
  }

  uint64_t RangeQuery(const KeyType& /*lower*/, const KeyType& /*upper*/,
                      uint32_t /*tid*/) const {
    return 0;
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t /*tid*/) {
    dpgm_.insert(kv.key, kv.value);
    shadow_.emplace_back(kv.key, kv.value);
    ++dpgm_count_;
    ++total_keys_;

    // Flush when DPGM holds >= 1/flush_threshold of total keys.
    if (dpgm_count_ * flush_threshold >= total_keys_) Flush();
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const {
    return lipp_.index_size() + dpgm_.size_in_bytes();
  }

  bool applicable(bool unique, bool /*range_query*/, bool /*insert*/,
                  bool multithread,
                  const std::string& /*ops_filename*/) const {
    // LIPP requires unique keys; neither LIPP nor DPGM supports multithread.
    // Also exclude LinearAVX (unsupported by DPGM's SearchClass).
    std::string sname = SearchClass::name();
    return unique && !multithread && sname != "LinearAVX";
  }

  std::vector<std::string> variants() const {
    return {SearchClass::name(), std::to_string(pgm_error),
            std::to_string(flush_threshold)};
  }

 private:
  void Flush() {
    // Insert all buffered keys into LIPP.
    for (const auto& kv : shadow_)
      lipp_.insert(kv.first, kv.second);

    // Reset DPGM and shadow buffer.
    dpgm_ = DPGMType();
    shadow_.clear();
    dpgm_count_ = 0;
  }

  LIPP<KeyType, uint64_t> lipp_;
  DPGMType dpgm_;
  // Shadow buffer mirrors DPGM contents for flushing (avoids broken begin()).
  std::vector<std::pair<KeyType, uint64_t>> shadow_;
  size_t total_keys_ = 0;
  size_t dpgm_count_ = 0;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
