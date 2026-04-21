#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "../util.h"
#include "base.h"
#include "lipp/src/core/lipp.h"
#include "pgm_index_dynamic.hpp"

// Workload-routed hybrid:
//   * Lookup-heavy (insert ratio < 0.5): pure LIPP. Inserts go straight
//     into LIPP; lookups probe LIPP directly.
//   * Insert-heavy (insert ratio >= 0.5): LIPP for bulk-loaded keys,
//     DPGM for new inserts. No flushing. Lookups probe LIPP first,
//     then fall back to DPGM.
//
// Workload type is detected by parsing the ops filename in applicable(),
// which the benchmark calls before Build().
template <class KeyType, class SearchClass, size_t pgm_error,
          size_t /*flush_threshold, unused*/ flush_threshold>
class HybridPGMLIPP : public Base<KeyType> {
  using DPGMType =
      DynamicPGMIndex<KeyType, uint64_t, SearchClass,
                      PGMIndex<KeyType, SearchClass, pgm_error, 16>>;

 public:
  HybridPGMLIPP(const std::vector<int>& /*params*/)
      : dpgm_(std::make_unique<DPGMType>()) {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t /*num_threads*/) {
    std::vector<std::pair<KeyType, uint64_t>> pairs;
    pairs.reserve(data.size());
    for (const auto& kv : data) pairs.emplace_back(kv.key, kv.value);

    return util::timing(
        [&] { lipp_.bulk_load(pairs.data(), (int)pairs.size()); });
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t /*tid*/) {
    if (insert_heavy_mode_) {
      dpgm_->insert(kv.key, kv.value);
    } else {
      lipp_.insert(kv.key, kv.value);
    }
  }

  size_t EqualityLookup(const KeyType& key, uint32_t /*tid*/) const {
    uint64_t value;
    if (lipp_.find(key, value)) return value;

    if (!insert_heavy_mode_) return util::OVERFLOW;

    auto it = dpgm_->find(key);
    if (it != dpgm_->end()) return it->value();
    return util::OVERFLOW;
  }

  uint64_t RangeQuery(const KeyType& /*lower*/, const KeyType& /*upper*/,
                      uint32_t /*tid*/) const {
    return 0;
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const {
    std::size_t s = lipp_.index_size();
    if (insert_heavy_mode_ && dpgm_) s += dpgm_->size_in_bytes();
    return s;
  }

  bool applicable(bool unique, bool /*range_query*/, bool /*insert*/,
                  bool multithread,
                  const std::string& ops_filename) const {
    double ratio = 0.0;
    auto ipos = ops_filename.find("i_");
    if (ipos != std::string::npos) {
      auto start = ops_filename.rfind('_', ipos - 1);
      if (start != std::string::npos) {
        try {
          ratio = std::stod(
              ops_filename.substr(start + 1, ipos - start - 1));
        } catch (...) {
          ratio = 0.0;
        }
      }
    }
    insert_heavy_mode_ = (ratio >= 0.5);

    std::string sname = SearchClass::name();
    return unique && !multithread && sname != "LinearAVX";
  }

  std::vector<std::string> variants() const {
    return {SearchClass::name(), std::to_string(pgm_error),
            std::to_string(flush_threshold)};
  }

 private:
  LIPP<KeyType, uint64_t> lipp_;
  std::unique_ptr<DPGMType> dpgm_;
  mutable bool insert_heavy_mode_ = false;
};

#endif  // TLI_HYBRID_PGM_LIPP_H