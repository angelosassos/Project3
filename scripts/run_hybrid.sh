#!/usr/bin/env bash

echo "Running Milestone 3 benchmarks..."

BENCHMARK=build/benchmark
# DATASETS="fb_100M_public_uint64 books_100M_public_uint64 osmc_100M_public_uint64"
DATASETS="fb_100M_public_uint64"
# INDEXES="DynamicPGM LIPP HybridPGMLIPP"
INDEXES="HybridPGMLIPP"

rm -f results

for DATASET in $DATASETS; do
    for INDEX in $INDEXES; do
        $BENCHMARK ./data/$DATASET ./data/${DATASET}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix --through --csv --only $INDEX -r 3
        $BENCHMARK ./data/$DATASET ./data/${DATASET}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix --through --csv --only $INDEX -r 3
    done
done

rm -f results

echo "Done! Results in ./results/"
