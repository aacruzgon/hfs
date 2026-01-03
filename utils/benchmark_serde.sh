#!/bin/bash

# Benchmark script to compare JSON serde performance between the current and main branches
# This script tests all FHIR versions (R4, R4B, R5, R6) against their respective JSON examples

set -e

BENCHMARK_RESULTS_DIR="./benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MAIN_RESULTS="$BENCHMARK_RESULTS_DIR/main_${TIMESTAMP}.txt"
REFACTOR_RESULTS="$BENCHMARK_RESULTS_DIR/refactor_${TIMESTAMP}.txt"
COMPARISON_RESULTS="$BENCHMARK_RESULTS_DIR/comparison_${TIMESTAMP}.txt"

# Create benchmark results directory
mkdir -p "$BENCHMARK_RESULTS_DIR"

echo "======================================"
echo "FHIR JSON Serde Performance Benchmark"
echo "======================================"
echo "Timestamp: $TIMESTAMP"
echo ""

# Function to run benchmarks for a specific branch
run_benchmark() {
    local branch=$1
    local output_file=$2

    echo "===============================================" | tee "$output_file"
    echo "Running benchmarks on branch: $branch" | tee -a "$output_file"
    echo "===============================================" | tee -a "$output_file"
    echo "" | tee -a "$output_file"

    # Checkout the branch
    git checkout "$branch" 2>&1 | tee -a "$output_file"
    echo "" | tee -a "$output_file"

    # Build the list of crates for this branch
    # The test_examples.rs location differs between branches (xml branch: helios-serde, main branch: helios-fhir)
    declare -a TEST_CRATES=()
    if [ -f "crates/serde/tests/test_examples.rs" ]; then
        TEST_CRATES+=("helios-serde|_json_examples|helios-fhir/skip-r6-download|test_examples")
        echo "Found test_examples in helios-serde" | tee -a "$output_file"
    fi
    if [ -f "crates/fhir/tests/test_examples.rs" ]; then
        TEST_CRATES+=("helios-fhir|_examples|skip-r6-download|test_examples")
        echo "Found test_examples in helios-fhir" | tee -a "$output_file"
    fi

    if [ ${#TEST_CRATES[@]} -eq 0 ]; then
        echo "ERROR: Unable to find test_examples.rs in any crate on branch $branch" | tee -a "$output_file"
        return 1
    fi
    echo "" | tee -a "$output_file"

    for crate_config in "${TEST_CRATES[@]}"; do
        IFS='|' read -r test_crate test_name_suffix r6_feature_flag test_target <<< "$crate_config"

        echo "-----------------------------------------------" | tee -a "$output_file"
        echo "Crate: $test_crate (test target: $test_target)" | tee -a "$output_file"
        echo "-----------------------------------------------" | tee -a "$output_file"

        echo "Cleaning previous build artifacts..." | tee -a "$output_file"
        cargo clean -p "$test_crate" 2>&1 | tee -a "$output_file"
        echo "" | tee -a "$output_file"

        if ! cargo test -p "$test_crate" --test "$test_target" --no-run --release >/dev/null 2>&1; then
            echo "WARNING: $test_crate does not have integration test '$test_target'. Skipping crate." | tee -a "$output_file"
            echo "" | tee -a "$output_file"
            continue
        fi

        local num_runs=100
        for version in R4 R4B R5 R6; do
            echo ">>> $test_crate - Testing FHIR version: $version ($num_runs runs)" | tee -a "$output_file"

            echo "Building $version tests..." | tee -a "$output_file"

            local cargo_features="$version"
            if [ "$version" = "R6" ] && [ -n "$r6_feature_flag" ]; then
                cargo_features="$cargo_features,$r6_feature_flag"
            fi

            cargo test -p "$test_crate" --no-default-features --features "$cargo_features" --test "$test_target" --release --no-run 2>&1 | tee -a "$output_file"
            echo "" | tee -a "$output_file"

            local test_binary=$(ls -t target/release/deps/${test_target}-* 2>/dev/null | grep -v '\.d$' | head -1)
            if [ -z "$test_binary" ]; then
                echo "ERROR: Could not find test binary for $test_crate/$version" | tee -a "$output_file"
                continue
            fi
            echo "Test binary: $test_binary" | tee -a "$output_file"
            echo "" | tee -a "$output_file"

            local version_lower=$(echo "$version" | tr '[:upper:]' '[:lower:]')
            local test_name="test_${version_lower}${test_name_suffix}"

            local total_time=0
            local run_times=()

            for ((i=1; i<=num_runs; i++)); do
                echo "Run $i/$num_runs..." | tee -a "$output_file"

                local start_time=$(date +%s.%N)

                if [ $i -eq 1 ]; then
                    "$test_binary" "$test_name" --nocapture 2>&1 | tee -a "$output_file"
                else
                    "$test_binary" "$test_name" --nocapture 2>&1 > /dev/null
                fi

                local end_time=$(date +%s.%N)
                local duration=$(echo "$end_time - $start_time" | bc)
                run_times+=("$duration")
                total_time=$(echo "$total_time + $duration" | bc)

                echo "  Run $i: ${duration} seconds" | tee -a "$output_file"
            done

            local avg_time=$(echo "scale=3; $total_time / $num_runs" | bc)

            echo "" | tee -a "$output_file"
            echo "All runs: ${run_times[*]}" | tee -a "$output_file"
            echo "Total time: ${total_time} seconds" | tee -a "$output_file"
            echo "✓ $test_crate $version tests completed (average): ${avg_time} seconds" | tee -a "$output_file"
            echo "RESULT,$branch,$test_crate,$version,$avg_time" | tee -a "$output_file"
            echo "" | tee -a "$output_file"
        done
    done

    echo "" | tee -a "$output_file"
    echo "Benchmark completed for branch: $branch" | tee -a "$output_file"
    echo "" | tee -a "$output_file"
}

# Function to extract timing information from results
extract_timings() {
    local file=$1
    local default_branch_label=$2

    echo "Extracting timings from $file"

    grep '^RESULT,' "$file" 2>/dev/null | while IFS=, read -r _ recorded_branch crate version timing; do
        local branch_label="$recorded_branch"
        if [ -n "$default_branch_label" ]; then
            branch_label="$default_branch_label"
        fi
        echo "$branch_label,$crate,$version,$timing"
    done
}

# Save current branch to return to it later
CURRENT_BRANCH=$(git branch --show-current)

# Run benchmarks on current branch first (before switching)
echo "Step 1/3: Benchmarking current branch ($CURRENT_BRANCH)..."
run_benchmark "$CURRENT_BRANCH" "$REFACTOR_RESULTS"

# Run benchmarks on main branch
echo "Step 2/3: Benchmarking main branch..."
run_benchmark "main" "$MAIN_RESULTS"

# Return to original branch
echo "Returning to original branch: $CURRENT_BRANCH"
git checkout "$CURRENT_BRANCH"

# Generate comparison report
echo "Step 3/3: Generating comparison report..."
echo "=====================================" | tee "$COMPARISON_RESULTS"
echo "Performance Comparison Report" | tee -a "$COMPARISON_RESULTS"
echo "=====================================" | tee -a "$COMPARISON_RESULTS"
echo "Timestamp: $TIMESTAMP" | tee -a "$COMPARISON_RESULTS"
echo "" | tee -a "$COMPARISON_RESULTS"

echo "Branch,Crate,Version,Time (seconds)" | tee -a "$COMPARISON_RESULTS"
echo "------,-----,-------,----------------" | tee -a "$COMPARISON_RESULTS"

# Extract and display timings
extract_timings "$MAIN_RESULTS" "main" | tee -a "$COMPARISON_RESULTS"
extract_timings "$REFACTOR_RESULTS" "refactor" | tee -a "$COMPARISON_RESULTS"

echo "" | tee -a "$COMPARISON_RESULTS"
echo "Detailed Results:" | tee -a "$COMPARISON_RESULTS"
echo "  Main branch:     $MAIN_RESULTS" | tee -a "$COMPARISON_RESULTS"
echo "  Refactor branch: $REFACTOR_RESULTS" | tee -a "$COMPARISON_RESULTS"
echo "" | tee -a "$COMPARISON_RESULTS"

# Calculate percentage differences
echo "Performance Differences (Refactor vs Main):" | tee -a "$COMPARISON_RESULTS"
echo "-------------------------------------------" | tee -a "$COMPARISON_RESULTS"

# Extract unique crate names from the results (handles both helios-serde and helios-fhir)
TESTED_CRATES=$(grep -h "^main," "$COMPARISON_RESULTS" "$COMPARISON_RESULTS" 2>/dev/null | cut -d',' -f2 | sort -u)
if [ -z "$TESTED_CRATES" ]; then
    # If no main results, try to get from refactor results
    TESTED_CRATES=$(grep -h "^refactor," "$COMPARISON_RESULTS" 2>/dev/null | cut -d',' -f2 | sort -u)
fi

for test_crate in $TESTED_CRATES; do
    for version in R4 R4B R5 R6; do
        main_time=$(grep "^main,$test_crate,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f4)
        refactor_time=$(grep "^refactor,$test_crate,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f4)

        if [ -n "$main_time" ] && [ -n "$refactor_time" ]; then
            diff=$(echo "scale=2; (($refactor_time - $main_time) / $main_time) * 100" | bc)

            if (( $(echo "$diff < 0" | bc -l) )); then
                echo "$test_crate $version: ${diff#-}% faster (IMPROVEMENT)" | tee -a "$COMPARISON_RESULTS"
            elif (( $(echo "$diff > 0" | bc -l) )); then
                echo "$test_crate $version: ${diff}% slower (REGRESSION)" | tee -a "$COMPARISON_RESULTS"
            else
                echo "$test_crate $version: No significant difference" | tee -a "$COMPARISON_RESULTS"
            fi
        else
            echo "$test_crate $version: Unable to compare (missing data)" | tee -a "$COMPARISON_RESULTS"
        fi
    done
done

echo "" | tee -a "$COMPARISON_RESULTS"
echo "=====================================" | tee -a "$COMPARISON_RESULTS"
echo "Benchmark Complete!" | tee -a "$COMPARISON_RESULTS"
echo "=====================================" | tee -a "$COMPARISON_RESULTS"
echo "" | tee -a "$COMPARISON_RESULTS"
echo "View comparison results at: $COMPARISON_RESULTS"
