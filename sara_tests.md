# Improving Testing in the Sara Project

This document outlines strategies and areas for improving the testing of the Sara Haskell project. Building upon the existing type safety and QuickCheck foundation, the goal is to increase confidence in correctness, robustness, and performance.

## 1. Expand Unit Test Coverage

Systematically identify and write explicit unit tests for all known edge cases for every function. This includes:

*   **Edge Cases:**
    *   Empty DataFrames as input.
    *   Single-row DataFrames.
    *   Columns containing only `NA` values.
    *   Numeric columns with all identical values (for standard deviation, variance, correlation).
    *   Input values that might lead to division by zero (e.g., empty groups for `meanAgg`).
    *   Boundary conditions for parameters (e.g., `alpha` values for `ewmApply` at 0.0 or 1.0, window sizes for rolling/expanding functions).
*   **Invalid Inputs:** Explicitly test how functions handle inputs that are syntactically correct but semantically invalid (e.g., attempting to perform numeric operations on a text column if `DFValue` allows it and conversion fails, or providing non-existent file paths to I/O functions). Ensure `SaraError` is returned as expected.
*   **Error Paths:** For every function that returns `Either SaraError a`, ensure that tests explicitly trigger and verify the `Left SaraError` cases with the correct error type and message.

## 2. Enhance Property-Based Testing (QuickCheck)

Formulate more comprehensive QuickCheck properties that capture deeper invariants and relationships between inputs and outputs.

*   **Richer `Arbitrary` Instances:** Develop more sophisticated `Arbitrary` instances for `DataFrame` and `Row` that can generate a wider, more diverse range of valid data. This includes:
    *   DataFrames with varying numbers of rows and columns.
    *   Columns with mixed data types (where `DFValue` allows).
    *   Strategic inclusion of `NA` values in generated data.
    *   Generation of data that specifically targets edge cases for statistical properties (e.g., nearly constant data for standard deviation tests).
*   **Stronger Properties:** Formulate more comprehensive QuickCheck properties that capture deeper invariants and relationships between inputs and outputs.
    *   **Inversion Properties:** `(f . g) x = x` (e.g., `readJSON . writeJSON` should be identity).
    *   **Commutativity/Associativity:** If applicable (e.g., `filterRows p1 (filterRows p2 df)` vs. `filterRows p2 (filterRows p1 df)`).
    *   **Monotonicity:** For sorting or cumulative functions.
    *   **Statistical Invariants:** For `minV`, `maxV`, `meanV`, ensure `minV <= meanV <= maxV`. For `correlate`, ensure the result is between -1 and 1.
    *   **Schema Preservation/Transformation:** Properties that verify the output DataFrame's schema (column names and types) is correct after transformations.
*   **Stateful Property Testing:** For functions that involve sequences of operations or maintain internal state (like rolling/expanding windows), consider using stateful property testing techniques (e.g., from `quickcheck-state-machine` or similar libraries) to test sequences of calls.

## 3. Integration Testing

Create tests that simulate realistic end-to-end data processing workflows, chaining multiple Sara functions together.

*   **Complex Workflows:** Test complex data pipelines (e.g., `readCsv` -> `filterRows` -> `mutate` -> `groupBy` -> `sumAgg` -> `writeJson`). Verify the final output against expected results.
*   **Large Data Handling:** Test with larger, representative datasets (e.g., 100,000+ rows) to ensure functions handle memory and performance gracefully, especially for streaming operations.
*   **File I/O Robustness:** Test reading and writing various CSV/JSON files, including malformed ones, files with different encodings, and very large files.

## 4. Performance Testing/Benchmarking

Use a dedicated benchmarking library like `criterion` to measure the performance of critical functions with varying data sizes. Track performance over time to detect regressions.

## 5. Regression Testing

Whenever a bug is found and fixed, write a specific regression test that reproduces the bug. This test should be added to the permanent test suite to prevent the bug from reappearing in the future.

## 6. Type-Level Testing (Advanced)

For very complex type-level logic, consider using advanced techniques or libraries (e.g., `type-errors` or `singletons`) to write explicit tests that verify specific type-level properties or ensure that certain *invalid* type combinations correctly produce compile-time errors with informative messages. This is highly specialized but can provide the strongest guarantees for type-level correctness.
