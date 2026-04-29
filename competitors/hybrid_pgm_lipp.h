#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../util.h"
#include "base.h"
#include "bloom_filter.h"
#include "lipp/src/core/lipp.h"
#include "pgm_index_dynamic.hpp"

template <class KeyType, class SearchClass, size_t pgm_error,
          size_t flush_threshold>
class HybridPGMLIPP : public Base<KeyType> {
  using DPGMType =
      DynamicPGMIndex<KeyType, uint64_t, SearchClass,
                      PGMIndex<KeyType, SearchClass, pgm_error, 16>>;

  // Bloom filter tuning:
  //   10 bits/key gives theoretical optimum at k = 10 * ln(2) ≈ 6.93.
  //   k=5 yields ~0.84% FPR, k=7 yields ~0.83% FPR — essentially no
  //   improvement from the two extra bit probes. Using k=5 for fewer
  //   memory accesses per lookup.
  static constexpr size_t kBloomBitsPerKey = 10;
  static constexpr size_t kBloomNumHashes = 5;

  // Cap the filter's sized-for-N so the bit array fits in cache rather
  // than spilling to main memory. At 100M bulk-load with flush_threshold=10,
  // the formula (total / flush_threshold) * 1.25 gives 12.5M entries -> a
  // 16 MB bit array (doesn't fit L2 or L3 cleanly, every bit probe risks
  // a cache miss). Capping at 2M entries gives ~2.5 MB -> fits L3 easily,
  // often fits L2. If the workload ever exceeds 2M buffered keys, FPR
  // degrades gracefully; correctness is unaffected.
  static constexpr size_t kBloomMaxExpected = 2'000'000;

 public:
  HybridPGMLIPP(const std::vector<int>& /*params*/)
      : dpgm_active_(std::make_unique<DPGMType>()),
        flush_in_progress_(false),
        shutdown_(false) {
    flush_thread_ = std::thread(&HybridPGMLIPP::BackgroundDrain, this);
  }

  ~HybridPGMLIPP() {
    {
      std::lock_guard<std::mutex> lk(flush_mtx_);
      shutdown_ = true;
    }
    flush_cv_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t /*num_threads*/) {
    std::vector<std::pair<KeyType, uint64_t>> pairs;
    pairs.reserve(data.size());
    for (const auto& kv : data) pairs.emplace_back(kv.key, kv.value);

    uint64_t t = util::timing(
        [&] { lipp_.bulk_load(pairs.data(), (int)pairs.size()); });

    total_keys_ = data.size();
    active_count_ = 0;

    expected_in_dpgm_ = (total_keys_ / flush_threshold);
    expected_in_dpgm_ += expected_in_dpgm_ / 4;
    if (expected_in_dpgm_ < 1024) expected_in_dpgm_ = 1024;
    // Cap so the Bloom fits in cache; see kBloomMaxExpected comment.
    if (expected_in_dpgm_ > kBloomMaxExpected)
      expected_in_dpgm_ = kBloomMaxExpected;
    bloom_active_.reset(expected_in_dpgm_, kBloomBitsPerKey, kBloomNumHashes);

    return t;
  }

  // LIPP-first lookup. Trusts LIPP's answer if LIPP finds the key.
  // CORRECTNESS ASSUMPTION: the workload does not re-insert keys that
  // already exist in LIPP. Verified empirically with --verify mode.
  size_t EqualityLookup(const KeyType& key, uint32_t /*tid*/) const {
    // Snapshot the flush state once so all stages see a consistent view.
    // Acquire pairs with the release in TryStartFlush() and
    // BackgroundDrain().
    const bool flush_active =
        flush_in_progress_.load(std::memory_order_acquire);

    // (1) Probe LIPP first. The vast majority of keys live here, so on
    // positive lookups for bulk-loaded keys this is the only probe we do.
    // Lock-free unless a flush is in progress; the release of
    // flush_in_progress_=false in the bg thread happens-after the final
    // LIPP write, so an acquire-load of false means we see a fully-written
    // LIPP state with no writer active.
    uint64_t lipp_value;
    bool lipp_found;
    if (flush_active) {
      std::lock_guard<std::mutex> lk(lipp_mtx_);
      lipp_found = lipp_.find(key, lipp_value);
    } else {
      lipp_found = lipp_.find(key, lipp_value);
    }
    if (lipp_found) return lipp_value;

    // (2) LIPP missed. Maybe the key is in the active DPGM (foreground
    // owns it exclusively, no lock needed). Skip the Bloom/DPGM probe
    // entirely if active is empty (e.g. a lookup-heavy run with no
    // inserts yet).
    if (active_count_ > 0 && bloom_active_.maybe_contains(key)) {
      auto it = dpgm_active_->find(key);
      if (it != dpgm_active_->end()) return it->value();
    }

    // (3) Frozen DPGM (only during a flush). bloom_frozen_ is safe to
    // read unlocked; dpgm_frozen_->find() needs flush_mtx_ because the
    // bg thread may destroy it at the end of the drain.
    if (flush_active && bloom_frozen_.maybe_contains(key)) {
      std::lock_guard<std::mutex> lk(flush_mtx_);
      if (dpgm_frozen_) {
        auto it = dpgm_frozen_->find(key);
        if (it != dpgm_frozen_->end()) return it->value();
      }
    }

    return util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& /*lower*/, const KeyType& /*upper*/,
                      uint32_t /*tid*/) const {
    return 0;
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t /*tid*/) {
    dpgm_active_->insert(kv.key, kv.value);
    bloom_active_.add(kv.key);
    ++active_count_;
    ++total_keys_;

    if (active_count_ * flush_threshold >= total_keys_ &&
        !flush_in_progress_.load(std::memory_order_acquire)) {
      TryStartFlush();
    }
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const {
    // PrintResult calls this while the bg thread may still be draining into
    // lipp_. LIPP::index_size() walks child pointers; a concurrent insert
    // that rebuilds a subtree would leave index_size() dereferencing freed
    // memory. Hold lipp_mtx_ here. Same reasoning for dpgm_frozen_.
    std::size_t s;
    {
      std::lock_guard<std::mutex> lk(lipp_mtx_);
      s = lipp_.index_size();
    }
    if (dpgm_active_) s += dpgm_active_->size_in_bytes();
    s += bloom_active_.size_in_bytes();
    {
      std::lock_guard<std::mutex> lk(flush_mtx_);
      if (dpgm_frozen_) s += dpgm_frozen_->size_in_bytes();
      s += bloom_frozen_.size_in_bytes();
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
  void TryStartFlush() {
    {
      std::lock_guard<std::mutex> lk(flush_mtx_);
      if (flush_in_progress_.load(std::memory_order_relaxed)) return;

      dpgm_frozen_ = std::move(dpgm_active_);
      dpgm_active_ = std::make_unique<DPGMType>();

      bloom_frozen_ = std::move(bloom_active_);
      bloom_active_ = BloomFilter();
      bloom_active_.reset(expected_in_dpgm_, kBloomBitsPerKey,
                          kBloomNumHashes);

      frozen_count_ = active_count_;
      active_count_ = 0;

      flush_in_progress_.store(true, std::memory_order_release);
    }
    flush_cv_.notify_one();
  }

  void BackgroundDrain() {
    while (true) {
      {
        std::unique_lock<std::mutex> lk(flush_mtx_);
        flush_cv_.wait(lk, [this] {
          return shutdown_ ||
                 flush_in_progress_.load(std::memory_order_relaxed);
        });
        if (shutdown_) return;
      }

      auto it =
          dpgm_frozen_->lower_bound(std::numeric_limits<KeyType>::min());
      while (it != dpgm_frozen_->end()) {
        {
          std::lock_guard<std::mutex> lk(lipp_mtx_);
          lipp_.insert(it->key(), it->value());
        }
        ++it;
      }

      {
        std::lock_guard<std::mutex> lk(flush_mtx_);
        dpgm_frozen_.reset();
        bloom_frozen_.clear();
        frozen_count_ = 0;
        flush_in_progress_.store(false, std::memory_order_release);
      }
    }
  }

  LIPP<KeyType, uint64_t> lipp_;
  std::unique_ptr<DPGMType> dpgm_active_;
  std::unique_ptr<DPGMType> dpgm_frozen_;
  BloomFilter bloom_active_;
  BloomFilter bloom_frozen_;

  size_t total_keys_ = 0;
  size_t active_count_ = 0;
  size_t frozen_count_ = 0;
  size_t expected_in_dpgm_ = 0;

  mutable std::mutex lipp_mtx_;
  mutable std::mutex flush_mtx_;
  std::condition_variable flush_cv_;
  std::atomic<bool> flush_in_progress_;
  std::atomic<bool> shutdown_;
  std::thread flush_thread_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H