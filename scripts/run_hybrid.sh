#!/usr/bin/env bash

echo "Running Milestone 2 benchmarks..."

BENCHMARK=build/benchmark
DATASET=fb_100M_public_uint64

mkdir -p results_milestone_2
rm -f results
ln -s results_milestone_2 results

for INDEX in HybridPGMLIPP; do
    $BENCHMARK ./data/$DATASET ./data/${DATASET}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix --through --csv --only $INDEX -r 3
    $BENCHMARK ./data/$DATASET ./data/${DATASET}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix --through --csv --only $INDEX -r 3
done

rm -f results

echo "Done! Results in ./results_milestone_2/"
