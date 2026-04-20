#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

template <typename Search, size_t eps>
static void run_thresholds(tli::Benchmark<uint64_t>& b) {
  b.template Run<HybridPGMLIPP<uint64_t, Search, eps, 10>>();
  b.template Run<HybridPGMLIPP<uint64_t, Search, eps, 100>>();
  b.template Run<HybridPGMLIPP<uint64_t, Search, eps, 1000>>();
  // b.template Run<HybridPGMLIPP<uint64_t, Search, eps, 10000>>();
  // b.template Run<HybridPGMLIPP<uint64_t, Search, eps, 100000>>();
}

template <typename Search>
static void sweep(tli::Benchmark<uint64_t>& b) {
  // run_thresholds<Search, 16>(b);
  // run_thresholds<Search, 32>(b);
  // run_thresholds<Search, 64>(b);
  // run_thresholds<Search, 128>(b);
  run_thresholds<Search, 256>(b);
  run_thresholds<Search, 512>(b);
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
  // sweep<BranchingBinarySearch<record>>(benchmark);
  sweep<LinearSearch<record>>(benchmark);
  // sweep<InterpolationSearch<record>>(benchmark);
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);