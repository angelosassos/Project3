#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "../util.h"
#include "base.h"
#include "lipp/src/core/lipp.h"
#include "pgm_index_dynamic.hpp"

// HybridPGMLIPP v2: double-buffered DPGM with Bloom-filter-gated lookups.
//
// Architecture:
//   - LIPP main store holds the bulk of keys (bulk-loaded initially).
//   - An "active" DPGM buffer absorbs new inserts.
//   - A "frozen" DPGM buffer is being incrementally drained into LIPP.
//   - Each buffer has an associated Bloom filter so lookups can skip the
//     expensive DynamicPGM search when a key is definitely not present.
//
// On Insert: key goes into the active buffer + active Bloom filter, then a
//   small batch of frozen keys is drained into LIPP (amortised flush).
//   When the active buffer reaches the threshold, buffers are swapped.
//
// On Lookup: active Bloom → active DPGM → frozen Bloom → frozen DPGM → LIPP.
//   The Bloom filter rejects ~99% of unnecessary DPGM searches.
template <class KeyType, class SearchClass, size_t pgm_error,
          size_t flush_threshold>
class HybridPGMLIPP : public Base<KeyType> {
  using DPGMType =
      DynamicPGMIndex<KeyType, uint64_t, SearchClass,
                      PGMIndex<KeyType, SearchClass, pgm_error, 16>>;
  using KVPair = std::pair<KeyType, uint64_t>;

  // --- Bloom filter (simple, header-only) -----------------------------------
  struct BloomFilter {
    std::vector<uint64_t> bits_;
    size_t num_bits_ = 0;

    static size_t mix1(KeyType k) {
      return static_cast<size_t>(static_cast<uint64_t>(k) *
                                 0x9E3779B97F4A7C15ULL);
    }
    static size_t mix2(KeyType k) {
      return static_cast<size_t>(static_cast<uint64_t>(k) *
                                 0x517CC1B727220A95ULL);
    }
    static size_t mix3(KeyType k) {
      return static_cast<size_t>(static_cast<uint64_t>(k) *
                                 0x6C62272E07BB0142ULL);
    }

    void init(size_t expected_keys) {
      num_bits_ = expected_keys * 10;
      if (num_bits_ < 64) num_bits_ = 64;
      bits_.assign((num_bits_ + 63) / 64, 0);
    }

    void insert(KeyType k) {
      if (num_bits_ == 0) return;
      size_t h1 = mix1(k) % num_bits_;
      size_t h2 = mix2(k) % num_bits_;
      size_t h3 = mix3(k) % num_bits_;
      bits_[h1 / 64] |= (1ULL << (h1 % 64));
      bits_[h2 / 64] |= (1ULL << (h2 % 64));
      bits_[h3 / 64] |= (1ULL << (h3 % 64));
    }

    bool maybe_contains(KeyType k) const {
      if (num_bits_ == 0) return false;
      size_t h1 = mix1(k) % num_bits_;
      size_t h2 = mix2(k) % num_bits_;
      size_t h3 = mix3(k) % num_bits_;
      return (bits_[h1 / 64] & (1ULL << (h1 % 64))) &&
             (bits_[h2 / 64] & (1ULL << (h2 % 64))) &&
             (bits_[h3 / 64] & (1ULL << (h3 % 64)));
    }

    void clear() { std::fill(bits_.begin(), bits_.end(), 0); }

    size_t size_in_bytes() const { return bits_.size() * sizeof(uint64_t); }
  };
  // --------------------------------------------------------------------------

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
    active_bloom_.init(expected_buf);
    frozen_bloom_.init(expected_buf);
    frozen_bloom_.clear();

    return t;
  }

  size_t EqualityLookup(const KeyType& key, uint32_t /*tid*/) const {
    if (active_bloom_.maybe_contains(key)) {
      auto it = active_dpgm_.find(key);
      if (it != active_dpgm_.end()) return it->value();
    }

    if (has_frozen_ && frozen_bloom_.maybe_contains(key)) {
      auto it = frozen_dpgm_.find(key);
      if (it != frozen_dpgm_.end()) return it->value();
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
    active_dpgm_.insert(kv.key, kv.value);
    active_shadow_.emplace_back(kv.key, kv.value);
    active_bloom_.insert(kv.key);
    ++active_count_;
    ++total_keys_;

    DrainBatch();

    if (active_count_ * flush_threshold >= total_keys_) SwapBuffers();
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const {
    size_t s = lipp_.index_size() + active_dpgm_.size_in_bytes();
    s += active_bloom_.size_in_bytes();
    if (has_frozen_) {
      s += frozen_dpgm_.size_in_bytes();
      s += frozen_bloom_.size_in_bytes();
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
    frozen_dpgm_ = DPGMType();
    frozen_shadow_.clear();
    frozen_bloom_.clear();
    frozen_cursor_ = 0;
    has_frozen_ = false;
  }

  void SwapBuffers() {
    if (has_frozen_) DrainAll();

    frozen_dpgm_ = std::move(active_dpgm_);
    frozen_shadow_ = std::move(active_shadow_);
    frozen_bloom_ = std::move(active_bloom_);
    frozen_cursor_ = 0;
    has_frozen_ = true;

    active_dpgm_ = DPGMType();
    active_shadow_.clear();
    active_shadow_.reserve(total_keys_ / flush_threshold + 1);
    active_bloom_.init(total_keys_ / flush_threshold + 1);
    active_count_ = 0;
  }

  // Main store
  LIPP<KeyType, uint64_t> lipp_;

  // Active buffer (absorbs new inserts)
  DPGMType active_dpgm_;
  std::vector<KVPair> active_shadow_;
  BloomFilter active_bloom_;
  size_t active_count_ = 0;

  // Frozen buffer (being incrementally drained into LIPP)
  DPGMType frozen_dpgm_;
  std::vector<KVPair> frozen_shadow_;
  BloomFilter frozen_bloom_;
  size_t frozen_cursor_ = 0;
  bool has_frozen_ = false;

  size_t total_keys_ = 0;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
