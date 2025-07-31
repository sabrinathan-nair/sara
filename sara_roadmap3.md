# Sara: Roadmap to Production-Level Robustness

This document outlines the next steps to elevate Sara to a production-ready state, focusing on advanced testing methodologies that go beyond our current unit and property-based tests. The goal is to ensure the library is not only type-safe but also formally verified, performant, and resilient to real-world failure modes.

## Phase 1: Enhanced Property-Based Testing

**Objective:** To uncover subtle bugs in the interaction between different DataFrame operations and to test more complex algebraic properties and stateful interactions.

**Granular Steps:**

1.  **Sophisticated `Arbitrary` Instances:**
    *   Create custom generators for `DataFrame`s that produce more challenging and diverse test cases.
    *   Generate DataFrames that are already sorted, have many duplicate values, or are entirely empty.
    *   Generate DataFrames with specific column types and value ranges to target edge cases in our functions.

2.  **Testing Algebraic Laws:**
    *   Write QuickCheck properties to verify that our functions adhere to expected algebraic laws.
    *   For example, test that `select("a", "b") >> select("c")` is equivalent to `select("a", "b", "c")`.
    *   Test that `sort` is idempotent (i.e., sorting an already sorted DataFrame produces the same DataFrame).

3.  **Stateful Testing:**
    *   Use `QuickCheck-SM` to model a `DataFrame` as a state machine.
    *   Generate long, complex sequences of operations (`filter`, `mutate`, `join`, `sort`, etc.) to see if any sequence of valid operations can lead to an invalid state.
    *   This will help us find bugs that only manifest after a specific sequence of operations.

## Phase 2: Expanded "Should-Not-Compile" Tests

**Objective:** To guarantee that user mistakes are caught by the compiler, providing a strong safety net for users of the library.

**Granular Steps:**

1.  **Create a dedicated test suite** for tests that are expected to fail compilation.
2.  **Add a test case for each type-level guarantee** we want to enforce, including:
    *   Trying to `sumAgg` a column of `Text`.
    *   Joining two DataFrames on columns of incompatible types (`Int` vs `Text`).
    *   Applying a function to a column that doesn't exist.
    *   Selecting a column that doesn't exist.
    *   Renaming a column to a name that already exists.
    *   Dropping a column that doesn't exist.

## Phase 3: Performance and Memory Profiling

**Objective:** To ensure the library is fast, scalable, and memory-efficient, especially our streaming implementation.

**Granular Steps:**

1.  **Benchmarking:**
    *   Use the `criterion` library to create a benchmark suite for our key functions.
    *   Run the benchmarks against DataFrames of varying sizes (e.g., 100 rows, 10,000 rows, 1,000,000 rows) to see how they scale.
    *   Identify and optimize any performance bottlenecks.

2.  **Memory Profiling:**
    *   Use GHC's built-in profiling tools (`+RTS -p -h`) to generate memory usage graphs for our streaming functions.
    *   Prove that functions like `filterRows` and `readCsvStreaming` have a constant (`O(1)`) memory footprint and do not load the entire dataset into memory at once.

## Phase 4: Formal Verification with Liquid Haskell

**Objective:** To mathematically *prove* correctness invariants at compile time, providing the highest level of assurance.

**Granular Steps:**

1.  **Integrate Liquid Haskell** into our build process.
2.  **Add Liquid Haskell annotations** to our function signatures and invariants, including:
    *   **`filterRows`:** Prove that the output DataFrame has a row count less than or equal to the input DataFrame.
    *   **`joinDF`:** Prove that the columns used for the join exist and have compatible types.
    *   **`sumAgg`:** Prove that this function is only ever called on a column whose type is numeric.
    *   **`readCsvStreaming`:** Prove that the output DataFrame's schema *exactly* matches the type-level schema provided, preventing runtime errors from malformed CSVs.
3.  **Refine our types** to be more specific and expressive, allowing us to prove more complex properties.

## Phase 5: Addressing Partial Functions and Robustness

**Objective:** To eliminate the use of partial functions and ensure robust handling of potential runtime failures, particularly concerning data parsing and manipulation.

**Granular Steps:**

1.  **Eliminate Partial `head` Usage:** (Completed)
    *   **Identified all instances** of `head` in the codebase.
    *   **Replaced `head` with safe alternatives** such as pattern matching, `Data.List.NonEmpty` (if applicable), `Data.Maybe.listToMaybe`, or explicit error handling (`Either`, `Maybe`).
    *   **Ensured all code paths** handle the case of empty lists gracefully.

2.  **Robust `readMaybe` Handling:** (Completed)
    *   **Identified all instances** of `readMaybe`.
    *   **Replaced implicit `Nothing` handling** with explicit `Maybe` or `Either` pattern matching.
    *   **Propagated parsing errors** up the call stack or converted them into meaningful `DFValue` representations (e.g., `NA`) where appropriate.
    *   **Used `Text.Read.readEither`** for more informative error messages during parsing.

3.  **Comprehensive Error Handling Strategy:** (Completed)
    *   **Defined a custom `SaraError` data type** to standardize error reporting.
    *   **Refactored existing error handling** (e.g., `error` calls) to use `SaraError`.
    *   **Resolved all type mismatches** in the test suite due to changes in function signatures.