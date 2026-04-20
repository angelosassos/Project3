#ifndef TLI_BLOOM_FILTER_H
#define TLI_BLOOM_FILTER_H

#include <cstdint>
#include <cstring>
#include <vector>

// A minimal Bloom filter used only as a query filter in HybridPGMLIPP.
// Stores NO keys or values — just a bit array. Used to cheaply answer
// "is this key possibly in DPGM?" on the lookup path.
//
// Implementation notes:
//  - Bit-array size is rounded up to a power of two so indexing uses a
//    bitmask instead of modulo.
//  - Uses the Kirsch-Mitzenmacher double-hashing trick: compute two base
//    hashes h1, h2, then synthesize the rest as h1 + i*h2. k hashes for
//    the cost of two.
class BloomFilter {
 public:
  BloomFilter() : m_bits_(0), m_mask_(0), num_hashes_(0) {}

  // Allocate bits for at most `expected_keys` at `bits_per_key` each.
  // Call once, before any add()/maybe_contains().
  void reset(size_t expected_keys, size_t bits_per_key, size_t num_hashes) {
    size_t target = expected_keys * bits_per_key;
    if (target < 64) target = 64;
    m_bits_ = 1;
    while (m_bits_ < target) m_bits_ <<= 1;
    m_mask_ = m_bits_ - 1;
    num_hashes_ = num_hashes;
    bits_.assign(m_bits_ / 64, 0);
  }

  // Zero out all bits without reallocating.
  void clear() {
    if (!bits_.empty())
      std::memset(bits_.data(), 0, bits_.size() * sizeof(uint64_t));
  }

  void add(uint64_t key) {
    if (m_bits_ == 0) return;
    uint64_t h1 = mix1(key);
    uint64_t h2 = mix2(key) | 1ULL;  // odd -> better bit diversity
    for (size_t i = 0; i < num_hashes_; ++i) {
      uint64_t h = (h1 + i * h2) & m_mask_;
      bits_[h >> 6] |= (1ULL << (h & 63));
    }
  }

  bool maybe_contains(uint64_t key) const {
    if (m_bits_ == 0) return true;  // untrained -> always "maybe"
    uint64_t h1 = mix1(key);
    uint64_t h2 = mix2(key) | 1ULL;
    for (size_t i = 0; i < num_hashes_; ++i) {
      uint64_t h = (h1 + i * h2) & m_mask_;
      if (!(bits_[h >> 6] & (1ULL << (h & 63)))) return false;
    }
    return true;
  }

  size_t size_in_bytes() const { return bits_.size() * sizeof(uint64_t); }

 private:
  // Two independent splitmix64-style mixers for double-hashing.
  static inline uint64_t mix1(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
  }
  static inline uint64_t mix2(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  }

  std::vector<uint64_t> bits_;
  size_t m_bits_;     // total bits, power of 2
  size_t m_mask_;     // m_bits_ - 1
  size_t num_hashes_;
};

#endif  // TLI_BLOOM_FILTER_H