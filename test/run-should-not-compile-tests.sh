#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

TEST_FILE="test/ShouldNotCompile/UpdateNonExistentColumn.hs"

echo "Attempting to compile $TEST_FILE..."

# Attempt to compile the file, redirecting stdout and stderr
if cabal build "$TEST_FILE" &> /dev/null; then
    echo -e "${RED}Error: $TEST_FILE compiled successfully, but it should have failed!${NC}"
    exit 1
else
    echo -e "${GREEN}Success: $TEST_FILE failed to compile as expected.${NC}"
    exit 0
fi
