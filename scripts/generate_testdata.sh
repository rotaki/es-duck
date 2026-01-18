#!/bin/bash
set -e

# Script to generate gensort testdata of specified size in GiB
# Usage: ./scripts/generate_testdata.sh <size_in_gib> <output_file>
# Example: ./scripts/generate_testdata.sh 5 testdata/test_gensort_5gb.dat

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <size_in_gib> <output_file>"
    echo "Example: $0 5 testdata/test_gensort_5gb.dat"
    exit 1
fi

SIZE_GIB=$1
OUTPUT_FILE=$2

# Each gensort record is 100 bytes (10-byte key + 90-byte payload)
RECORD_SIZE=100

# Calculate number of records needed
# 1 GiB = 1024^3 bytes = 1,073,741,824 bytes
BYTES_PER_GIB=1073741824
TOTAL_BYTES=$(echo "$SIZE_GIB * $BYTES_PER_GIB" | bc)
NUM_RECORDS=$(echo "$TOTAL_BYTES / $RECORD_SIZE" | bc)

echo "Generating ${SIZE_GIB} GiB of gensort testdata..."
echo "Records to generate: ${NUM_RECORDS}"
echo "Output file: ${OUTPUT_FILE}"
echo ""

# Create testdata directory if it doesn't exist
mkdir -p "$(dirname "$OUTPUT_FILE")"

# Build the generator if needed
echo "Building generate-gensort binary..."
cargo build --release --bin generate-gensort

# Run the generator
echo "Generating data..."
./target/release/generate-gensort \
    --output "$OUTPUT_FILE" \
    --num-records "$NUM_RECORDS"

echo ""
echo "Generation complete!"
ls -lh "$OUTPUT_FILE"
