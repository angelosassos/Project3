#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

template <int record, size_t eps>
static void run_thresholds(tli::Benchmark<uint64_t>& b) {
  b.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, eps, 10>>();
  b.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, eps, 20>>();
  b.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, eps, 50>>();
}

template <int record>
static void sweep(tli::Benchmark<uint64_t>& b) {
  run_thresholds<record, 64>(b);
  run_thresholds<record, 128>(b);
  run_thresholds<record, 256>(b);
  run_thresholds<record, 512>(b);
}

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                   bool pareto, const std::vector<int>& params) {
  if (!pareto) {
    util::fail("Hybrid PGM+LIPP's hyperparameter cannot be set");
  } else {
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16, 20>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 32, 20>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 64, 20>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 128, 20>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 256, 20>>();
  }
}

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                   const std::string& filename) {
  sweep<record>(benchmark);
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
