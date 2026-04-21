#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

template <int record>
static void sweep(tli::Benchmark<uint64_t>& b) {
  // Only the search variant and pgm_error matter now; flush_threshold
  // is a vestigial template parameter.
  b.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 256, 10>>();
  b.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 512, 10>>();
}

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  bool pareto,
                                  const std::vector<int>& params) {
  if (!pareto) {
    util::fail("Hybrid PGM+LIPP's hyperparameter cannot be set");
  } else {
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16, 10>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 32, 10>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 64, 10>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 128, 10>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 256, 10>>();
  }
}

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  const std::string& filename) {
  sweep<record>(benchmark);
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);