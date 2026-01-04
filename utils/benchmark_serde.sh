#!/bin/bash

# Benchmark script to compare JSON serde performance between branches
# This script uses git worktree to avoid conflicts with local changes
# Usage:
#   ./benchmark_serde.sh <branch1> [branch2]              # Run benchmarks on specified branches
#   ./benchmark_serde.sh <branch> --use <existing_file>   # Run branch and compare with existing results

set -e

BENCHMARK_RESULTS_DIR="./benchmark_results"
WORKTREE_DIR=".benchmark_worktrees"
ALL_FHIR_VERSIONS=("R4" "R4B" "R5" "R6")
FHIR_VERSIONS=("${ALL_FHIR_VERSIONS[@]}")

# Parse command line arguments
BRANCHES=()
USE_EXISTING=""
COMPARE_MODE=false
COMPARE_FILE1=""
COMPARE_FILE2=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --use)
            USE_EXISTING="$2"
            shift 2
            ;;
        --compare)
            COMPARE_MODE=true
            COMPARE_FILE1="$2"
            COMPARE_FILE2="$3"
            shift 3
            ;;
        --fhir-version|-F)
            if [ -z "${2:-}" ]; then
                echo "ERROR: --fhir-version requires a value (comma-separated list of versions)"
                exit 1
            fi
            IFS=',' read -ra requested_versions <<< "$2"
            if [ ${#requested_versions[@]} -eq 0 ]; then
                echo "ERROR: --fhir-version requires at least one version"
                exit 1
            fi
            FHIR_VERSIONS=()
            for version in "${requested_versions[@]}"; do
                version_clean=$(echo "$version" | tr -d '[:space:]')
                if [ -z "$version_clean" ]; then
                    continue
                fi
                version_upper=$(echo "$version_clean" | tr '[:lower:]' '[:upper:]')
                case "$version_upper" in
                    R4|R4B|R5|R6)
                        FHIR_VERSIONS+=("$version_upper")
                        ;;
                    *)
                        echo "ERROR: Unsupported FHIR version '$version'. Supported versions: ${ALL_FHIR_VERSIONS[*]}"
                        exit 1
                        ;;
                esac
            done
            if [ ${#FHIR_VERSIONS[@]} -eq 0 ]; then
                echo "ERROR: --fhir-version did not include any valid versions"
                exit 1
            fi
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [branch1] [branch2] [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --use <result_file>           Use existing result file for comparison"
            echo "  --compare <file1> <file2>     Compare two existing result files"
            echo "  --fhir-version <versions>     Comma-separated list of FHIR releases to benchmark (default: ${ALL_FHIR_VERSIONS[*]})"
            echo "  --help, -h                    Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                            # Run current branch + main (default)"
            echo "  $0 xml main                   # Run benchmarks on xml and main branches"
            echo "  $0 xml                        # Run benchmark on xml branch only"
            echo "  $0 main --use xml_20260103.txt  # Run main and compare with existing xml results"
            echo "  $0 xml --fhir-version R4,R5     # Limit benchmark to specific FHIR releases"
            echo "  $0 --compare xml_20260103.txt main_20260103.txt  # Just compare two existing files"
            echo ""
            echo "Result files are named: <branch>_<timestamp>.txt"
            exit 0
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Use '$0 --help' to see usage"
            exit 1
            ;;
        *)
            BRANCHES+=("$1")
            shift
            ;;
    esac
done

# Default: run current branch + main if no branches were provided (even if options were)
if [ "$COMPARE_MODE" = false ] && [ ${#BRANCHES[@]} -eq 0 ]; then
    CURRENT_BRANCH=$(git branch --show-current)
    BRANCHES=("$CURRENT_BRANCH" "main")
    echo "No branches provided. Running benchmarks for current branch ($CURRENT_BRANCH) and main"
fi

# Handle compare-only mode
if [ "$COMPARE_MODE" = true ]; then
    if [ ! -f "$COMPARE_FILE1" ] || [ ! -f "$COMPARE_FILE2" ]; then
        echo "ERROR: One or both comparison files not found"
        echo "  File 1: $COMPARE_FILE1"
        echo "  File 2: $COMPARE_FILE2"
        exit 1
    fi

    # Extract timestamp from first file
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    if [[ "$COMPARE_FILE1" =~ _([0-9_]+)\.txt ]]; then
        TIMESTAMP="${BASH_REMATCH[1]}"
    fi

    # Extract branch names from filenames
    branch1=$(basename "$COMPARE_FILE1" | sed 's/_[0-9_]*\.txt$//')
    branch2=$(basename "$COMPARE_FILE2" | sed 's/_[0-9_]*\.txt$//')

    mkdir -p "$BENCHMARK_RESULTS_DIR"
    COMPARISON_RESULTS="$BENCHMARK_RESULTS_DIR/comparison_${TIMESTAMP}.txt"

    echo "======================================"
    echo "Comparing Benchmark Results"
    echo "======================================"
    echo "File 1 ($branch1): $COMPARE_FILE1"
    echo "File 2 ($branch2): $COMPARE_FILE2"
    echo ""

    # Generate comparison report (reuse the comparison logic)
    echo "=====================================" | tee "$COMPARISON_RESULTS"
    echo "Performance Comparison Report" | tee -a "$COMPARISON_RESULTS"
    echo "=====================================" | tee -a "$COMPARISON_RESULTS"
    echo "Timestamp: $TIMESTAMP" | tee -a "$COMPARISON_RESULTS"
    echo "" | tee -a "$COMPARISON_RESULTS"

    echo "Branch,Crate,Version,Time (seconds)" | tee -a "$COMPARISON_RESULTS"
    echo "------,-----,-------,----------------" | tee -a "$COMPARISON_RESULTS"

    # Extract timings
    grep '^RESULT,' "$COMPARE_FILE1" 2>/dev/null | while IFS=, read -r _ _ crate version timing; do
        echo "$branch1,$crate,$version,$timing"
    done | tee -a "$COMPARISON_RESULTS"

    grep '^RESULT,' "$COMPARE_FILE2" 2>/dev/null | while IFS=, read -r _ _ crate version timing; do
        echo "$branch2,$crate,$version,$timing"
    done | tee -a "$COMPARISON_RESULTS"

    echo "" | tee -a "$COMPARISON_RESULTS"
    echo "Detailed Results:" | tee -a "$COMPARISON_RESULTS"
    echo "  $branch1: $COMPARE_FILE1" | tee -a "$COMPARISON_RESULTS"
    echo "  $branch2: $COMPARE_FILE2" | tee -a "$COMPARISON_RESULTS"
    echo "" | tee -a "$COMPARISON_RESULTS"

    echo "Performance Differences ($branch2 vs $branch1):" | tee -a "$COMPARISON_RESULTS"
    echo "-------------------------------------------" | tee -a "$COMPARISON_RESULTS"

    # Compare by FHIR version only (crate names may differ between branches)
    for version in "${FHIR_VERSIONS[@]}"; do
        # Get timings for each branch (any crate name)
        time1=$(grep "^$branch1,.*,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f4)
        time2=$(grep "^$branch2,.*,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f4)

        if [ -n "$time1" ] && [ -n "$time2" ]; then
            # Get crate names for display
            crate1=$(grep "^$branch1,.*,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f2)
            crate2=$(grep "^$branch2,.*,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f2)

            diff=$(echo "scale=2; (($time2 - $time1) / $time1) * 100" | bc)

            if (( $(echo "$diff < 0" | bc -l) )); then
                echo "$version: ${diff#-}% faster ($branch2:$crate2 vs $branch1:$crate1) - IMPROVEMENT" | tee -a "$COMPARISON_RESULTS"
            elif (( $(echo "$diff > 0" | bc -l) )); then
                echo "$version: ${diff}% slower ($branch2:$crate2 vs $branch1:$crate1) - REGRESSION" | tee -a "$COMPARISON_RESULTS"
            else
                echo "$version: No significant difference ($branch2:$crate2 vs $branch1:$crate1)" | tee -a "$COMPARISON_RESULTS"
            fi
        fi
    done

    echo "" | tee -a "$COMPARISON_RESULTS"
    echo "=====================================" | tee -a "$COMPARISON_RESULTS"
    echo "Comparison Complete!" | tee -a "$COMPARISON_RESULTS"
    echo "=====================================" | tee -a "$COMPARISON_RESULTS"
    echo "" | tee -a "$COMPARISON_RESULTS"
    echo "View comparison results at: $COMPARISON_RESULTS"

    exit 0
fi

# Generate timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# If using existing results, extract timestamp if possible
if [ -n "$USE_EXISTING" ]; then
    if [[ "$USE_EXISTING" =~ _([0-9_]+)\.txt ]]; then
        TIMESTAMP="${BASH_REMATCH[1]}"
    fi
fi

# Create benchmark results directory
mkdir -p "$BENCHMARK_RESULTS_DIR"

echo "======================================"
echo "FHIR JSON Serde Performance Benchmark"
echo "======================================"
echo "Timestamp: $TIMESTAMP"
echo "Branches to benchmark: ${BRANCHES[*]}"
echo "FHIR versions: ${FHIR_VERSIONS[*]}"
if [ -n "$USE_EXISTING" ]; then
    echo "Using existing results: $USE_EXISTING"
fi
echo ""

# Function to cleanup worktrees
cleanup_worktrees() {
    echo "Cleaning up worktrees..."
    if [ -d "$WORKTREE_DIR" ]; then
        for worktree in "$WORKTREE_DIR"/*; do
            if [ -d "$worktree" ]; then
                branch_name=$(basename "$worktree")
                echo "Removing worktree: $branch_name"
                git worktree remove "$worktree" --force 2>/dev/null || true
            fi
        done
        rmdir "$WORKTREE_DIR" 2>/dev/null || true
    fi
}

# Set trap to cleanup on exit
trap cleanup_worktrees EXIT

# Function to run benchmarks for a specific branch
run_benchmark() {
    local branch=$1
    local output_file=$2
    local worktree_path="$WORKTREE_DIR/$branch"
    local current_branch=$(git branch --show-current)
    local use_worktree=true

    echo "===============================================" | tee "$output_file"
    echo "Running benchmarks on branch: $branch" | tee -a "$output_file"
    echo "===============================================" | tee -a "$output_file"
    echo "" | tee -a "$output_file"

    # Check if we're already on the target branch
    if [ "$branch" = "$current_branch" ]; then
        echo "Already on branch $branch, running benchmark in current directory" | tee -a "$output_file"
        echo "Working directory: $(pwd)" | tee -a "$output_file"
        use_worktree=false
        worktree_path="$ORIGINAL_DIR"
    else
        # Create worktree for this branch
        echo "Creating worktree for branch $branch at $worktree_path..." | tee -a "$output_file"
        rm -rf "$worktree_path"
        mkdir -p "$WORKTREE_DIR"
        git worktree add "$worktree_path" "$branch" 2>&1 | tee -a "$output_file"
    fi
    echo "" | tee -a "$output_file"

    # Change to worktree directory
    cd "$worktree_path"

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
        cd - > /dev/null
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
        for version in "${FHIR_VERSIONS[@]}"; do
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

    # Return to original directory if we created a worktree
    if [ "$use_worktree" = true ]; then
        cd - > /dev/null
    fi
}

# Function to extract timing information from results
extract_timings() {
    local file=$1
    local branch_label=$2

    echo "Extracting timings from $file"

    grep '^RESULT,' "$file" 2>/dev/null | while IFS=, read -r _ recorded_branch crate version timing; do
        echo "$branch_label,$crate,$version,$timing"
    done
}

# Get current directory (to use for absolute paths)
ORIGINAL_DIR=$(pwd)
WORKTREE_DIR="$ORIGINAL_DIR/$WORKTREE_DIR"

# Collect result files (using parallel arrays for bash 3.2 compatibility)
RESULT_BRANCHES=()
RESULT_FILES=()

# Add existing result file if specified
if [ -n "$USE_EXISTING" ]; then
    # Extract branch name from filename (assumes format: branch_timestamp.txt)
    existing_branch=$(basename "$USE_EXISTING" | sed 's/_[0-9_]*\.txt$//')
    RESULT_BRANCHES+=("$existing_branch")
    RESULT_FILES+=("$ORIGINAL_DIR/$USE_EXISTING")
    echo "Using existing results for branch '$existing_branch': $USE_EXISTING"
fi

# Run benchmarks for specified branches
for branch in "${BRANCHES[@]}"; do
    result_file="$ORIGINAL_DIR/$BENCHMARK_RESULTS_DIR/${branch}_${TIMESTAMP}.txt"
    RESULT_BRANCHES+=("$branch")
    RESULT_FILES+=("$result_file")

    echo "Benchmarking branch: $branch"
    run_benchmark "$branch" "$result_file"
done

# Generate comparison report if we have multiple results
if [ ${#RESULT_FILES[@]} -ge 2 ]; then
    COMPARISON_RESULTS="$ORIGINAL_DIR/$BENCHMARK_RESULTS_DIR/comparison_${TIMESTAMP}.txt"

    echo "Generating comparison report..."
    echo "=====================================" | tee "$COMPARISON_RESULTS"
    echo "Performance Comparison Report" | tee -a "$COMPARISON_RESULTS"
    echo "=====================================" | tee -a "$COMPARISON_RESULTS"
    echo "Timestamp: $TIMESTAMP" | tee -a "$COMPARISON_RESULTS"
    echo "" | tee -a "$COMPARISON_RESULTS"

    echo "Branch,Crate,Version,Time (seconds)" | tee -a "$COMPARISON_RESULTS"
    echo "------,-----,-------,----------------" | tee -a "$COMPARISON_RESULTS"

    # Extract and display timings for all branches
    for i in "${!RESULT_BRANCHES[@]}"; do
        branch="${RESULT_BRANCHES[$i]}"
        result_file="${RESULT_FILES[$i]}"
        extract_timings "$result_file" "$branch" | tee -a "$COMPARISON_RESULTS"
    done

    echo "" | tee -a "$COMPARISON_RESULTS"
    echo "Detailed Results:" | tee -a "$COMPARISON_RESULTS"
    for i in "${!RESULT_BRANCHES[@]}"; do
        echo "  ${RESULT_BRANCHES[$i]}: ${RESULT_FILES[$i]}" | tee -a "$COMPARISON_RESULTS"
    done
    echo "" | tee -a "$COMPARISON_RESULTS"

    # Calculate percentage differences (assumes first two branches to compare)
    echo "Performance Differences:" | tee -a "$COMPARISON_RESULTS"
    echo "-------------------------------------------" | tee -a "$COMPARISON_RESULTS"

    if [ ${#RESULT_BRANCHES[@]} -ge 2 ]; then
        branch1="${RESULT_BRANCHES[0]}"
        branch2="${RESULT_BRANCHES[1]}"

        echo "Comparing $branch2 vs $branch1:" | tee -a "$COMPARISON_RESULTS"
        echo "" | tee -a "$COMPARISON_RESULTS"

        # Compare by FHIR version only (crate names may differ between branches)
        for version in "${FHIR_VERSIONS[@]}"; do
            # Get timings for each branch (any crate name)
            time1=$(grep "^$branch1,.*,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f4)
            time2=$(grep "^$branch2,.*,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f4)

            if [ -n "$time1" ] && [ -n "$time2" ]; then
                # Get crate names for display
                crate1=$(grep "^$branch1,.*,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f2)
                crate2=$(grep "^$branch2,.*,$version," "$COMPARISON_RESULTS" | tail -1 | cut -d',' -f2)

                diff=$(echo "scale=2; (($time2 - $time1) / $time1) * 100" | bc)

                if (( $(echo "$diff < 0" | bc -l) )); then
                    echo "$version: ${diff#-}% faster ($branch2:$crate2 vs $branch1:$crate1) - IMPROVEMENT" | tee -a "$COMPARISON_RESULTS"
                elif (( $(echo "$diff > 0" | bc -l) )); then
                    echo "$version: ${diff}% slower ($branch2:$crate2 vs $branch1:$crate1) - REGRESSION" | tee -a "$COMPARISON_RESULTS"
                else
                    echo "$version: No significant difference ($branch2:$crate2 vs $branch1:$crate1)" | tee -a "$COMPARISON_RESULTS"
                fi
            fi
        done
    fi

    echo "" | tee -a "$COMPARISON_RESULTS"
    echo "=====================================" | tee -a "$COMPARISON_RESULTS"
    echo "Benchmark Complete!" | tee -a "$COMPARISON_RESULTS"
    echo "=====================================" | tee -a "$COMPARISON_RESULTS"
    echo "" | tee -a "$COMPARISON_RESULTS"
    echo "View comparison results at: $COMPARISON_RESULTS"
else
    echo "Single branch benchmarked. Results saved to:"
    for i in "${!RESULT_BRANCHES[@]}"; do
        echo "  ${RESULT_BRANCHES[$i]}: ${RESULT_FILES[$i]}"
    done
fi
