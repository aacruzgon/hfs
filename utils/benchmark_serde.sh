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

    # Clean build to ensure fresh compilation
    echo "Cleaning previous build..." | tee -a "$output_file"
    cargo clean -p helios-fhir 2>&1 | tee -a "$output_file"
    echo "" | tee -a "$output_file"

    # Detect which test target name is available (test_examples vs test_json_examples)
    local test_target="test_examples"
    if cargo test -p helios-fhir --test test_json_examples --no-run 2>&1 | grep -q "no test target named"; then
        test_target="test_examples"
        echo "Using test target: test_examples" | tee -a "$output_file"
    else
        # Check if test_json_examples exists
        if ! cargo test -p helios-fhir --test test_examples --no-run 2>&1 | grep -q "no test target named"; then
            test_target="test_examples"
            echo "Using test target: test_examples" | tee -a "$output_file"
        else
            test_target="test_json_examples"
            echo "Using test target: test_json_examples" | tee -a "$output_file"
        fi
    fi
    echo "" | tee -a "$output_file"

    # Test each FHIR version separately (run each 100 times for better statistics)
    local num_runs=100
    for version in R4 R4B R5 R6; do
        echo "-----------------------------------------------" | tee -a "$output_file"
        echo "Testing FHIR version: $version ($num_runs runs)" | tee -a "$output_file"
        echo "-----------------------------------------------" | tee -a "$output_file"

        # Build this specific version (not timed)
        echo "Building $version tests..." | tee -a "$output_file"

        # For R6, add skip-r6-download feature to prevent downloading test data
        if [ "$version" = "R6" ]; then
            cargo test -p helios-fhir --no-default-features --features "$version,skip-r6-download" --test "$test_target" --release --no-run 2>&1 | tee -a "$output_file"
        else
            cargo test -p helios-fhir --no-default-features --features "$version" --test "$test_target" --release --no-run 2>&1 | tee -a "$output_file"
        fi
        echo "" | tee -a "$output_file"

        # Find the most recently compiled test binary (sorted by modification time)
        local test_binary=$(ls -t target/release/deps/${test_target}-* 2>/dev/null | grep -v '\.d$' | head -1)
        if [ -z "$test_binary" ]; then
            echo "ERROR: Could not find test binary for $version" | tee -a "$output_file"
            continue
        fi
        echo "Test binary: $test_binary" | tee -a "$output_file"
        echo "" | tee -a "$output_file"

        # Convert version to lowercase for test name
        local version_lower=$(echo "$version" | tr '[:upper:]' '[:lower:]')
        local test_name="test_${version_lower}_examples"

        # Run the test multiple times and collect timings
        local total_time=0
        local run_times=()

        for ((i=1; i<=num_runs; i++)); do
            echo "Run $i/$num_runs..." | tee -a "$output_file"

            local start_time=$(date +%s.%N)

            # Run test binary directly (suppress output except for first run to reduce log size)
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

        # Calculate average
        local avg_time=$(echo "scale=3; $total_time / $num_runs" | bc)

        echo "" | tee -a "$output_file"
        echo "All runs: ${run_times[*]}" | tee -a "$output_file"
        echo "Total time: ${total_time} seconds" | tee -a "$output_file"
        echo "✓ $version tests completed (average): ${avg_time} seconds" | tee -a "$output_file"
        echo "" | tee -a "$output_file"
    done

    echo "" | tee -a "$output_file"
    echo "Benchmark completed for branch: $branch" | tee -a "$output_file"
    echo "" | tee -a "$output_file"
}

# Function to extract timing information from results
extract_timings() {
    local file=$1
    local branch=$2

    echo "Extracting timings from $file for branch $branch"

    for version in R4 R4B R5 R6; do
        local timing=$(grep "✓ $version tests completed (average):" "$file" 2>/dev/null | sed 's/.*average): \([0-9.]*\) seconds.*/\1/' || echo "N/A")
        echo "$branch,$version,$timing"
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

echo "Branch,Version,Time (seconds)" | tee -a "$COMPARISON_RESULTS"
echo "-----,-------,----------------" | tee -a "$COMPARISON_RESULTS"

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

for version in R4 R4B R5 R6; do
    main_time=$(grep "main,$version," "$COMPARISON_RESULTS" | cut -d',' -f3)
    refactor_time=$(grep "refactor,$version," "$COMPARISON_RESULTS" | cut -d',' -f3)

    if [ "$main_time" != "N/A" ] && [ "$refactor_time" != "N/A" ]; then
        diff=$(echo "scale=2; (($refactor_time - $main_time) / $main_time) * 100" | bc)

        if (( $(echo "$diff < 0" | bc -l) )); then
            echo "$version: ${diff#-}% faster (IMPROVEMENT)" | tee -a "$COMPARISON_RESULTS"
        elif (( $(echo "$diff > 0" | bc -l) )); then
            echo "$version: ${diff}% slower (REGRESSION)" | tee -a "$COMPARISON_RESULTS"
        else
            echo "$version: No significant difference" | tee -a "$COMPARISON_RESULTS"
        fi
    else
        echo "$version: Unable to compare (missing data)" | tee -a "$COMPARISON_RESULTS"
    fi
done

echo "" | tee -a "$COMPARISON_RESULTS"
echo "=====================================" | tee -a "$COMPARISON_RESULTS"
echo "Benchmark Complete!" | tee -a "$COMPARISON_RESULTS"
echo "=====================================" | tee -a "$COMPARISON_RESULTS"
echo "" | tee -a "$COMPARISON_RESULTS"
echo "View comparison results at: $COMPARISON_RESULTS"
