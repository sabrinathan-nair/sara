# Sara Project Progress Report

## Analysis of Sara Project

The Sara project aims to provide a type-safe and performant DataFrame library in Haskell, leveraging type-level programming for compile-time guarantees. The core `DataFrame` type is parameterized by its column names, enabling strong type safety for schema-related operations. Existing functions like `selectColumns` and `dropColumns` successfully demonstrate this type-safe approach by manipulating the schema at the type level.

However, the implementation has been inconsistent. Some functions, particularly those involving row-wise operations or data ingestion, have historically fallen back to runtime checks using `Maybe` or `DFValue` types. This can lead to runtime errors, undermining the goal of compile-time safety.

The project is currently undergoing a refactoring effort to align all DataFrame operations with the type-safe patterns, ensuring that functions return `Either SaraError` for explicit error handling instead of relying on `Maybe` or partial functions like `fromJust`/`fromMaybe`. Performance improvements through streaming are also a key focus, with the `streaming` library integrated. Property-based testing with QuickCheck is also being utilized to enhance test coverage and ensure correctness.

## Pending Work

### Least Effort:

*   **Phase 2: Expanded "Should-Not-Compile" Tests**
    *   Create a dedicated test suite for tests that are expected to fail compilation.
    *   Add a test case for each type-level guarantee we want to enforce, including:
        *   Trying to `sumAgg` a column of `Text`.
        *   Joining two DataFrames on columns of incompatible types (`Int` vs `Text`).
        *   Applying a function to a column that doesn't exist.
        *   Selecting a column that doesn't exist.
        *   Renaming a column to a name that already exists.
        *   Dropping a column that doesn't exist.

*   **Phase 1: Enhanced Property-Based Testing - Sophisticated `Arbitrary` Instances**
    *   Create custom generators for `DataFrame`s that produce more challenging and diverse test cases.
    *   Generate DataFrames that are already sorted, have many duplicate values, or are entirely empty.
    *   Generate DataFrames with specific column types and value ranges to target edge cases in our functions.

### Medium Effort:

*   **Phase 1: Enhanced Property-Based Testing - Testing Algebraic Laws**
    *   Write QuickCheck properties to verify that our functions adhere to expected algebraic laws.
    *   For example, test that `select("a", "b") >> select("c")` is equivalent to `select("a", "b", "c")`.
    *   Test that `sort` is idempotent (i.e., sorting an already sorted DataFrame produces the same DataFrame).

*   **Phase 3: Performance and Memory Profiling - Benchmarking**
    *   Use the `criterion` library to create a benchmark suite for our key functions.
    *   Run the benchmarks against DataFrames of varying sizes (e.g., 100 rows, 10,000 rows, 1,000,000 rows) to see how they scale.
    *   Identify and optimize any performance bottlenecks.

### Most Effort:

*   **Phase 1: Enhanced Property-Based Testing - Stateful Testing**
    *   Use `QuickCheck-SM` to model a `DataFrame` as a state machine.
    *   Generate long, complex sequences of operations (`filter`, `mutate`, `join`, `sort`, etc.) to see if any sequence of valid operations can lead to an invalid state.

*   **Phase 3: Performance and Memory Profiling - Memory Profiling**
    *   Use GHC's built-in profiling tools (`+RTS -p -h`) to generate memory usage graphs for our streaming functions.
    *   Prove that functions like `filterRows` and `readCsvStreaming` have a constant (`O(1)`) memory footprint and do not load the entire dataset into memory at once.

*   **Phase 4: Formal Verification with Liquid Haskell**
    *   Integrate Liquid Haskell into our build process.
    *   Add Liquid Haskell annotations to our function signatures and invariants, including:
        *   **`filterRows`:** Prove that the output DataFrame has a row count less than or equal to the input DataFrame.
        *   **`joinDF`:** Prove that the columns used for the join exist and have compatible types.
        *   **`sumAgg`:** Prove that this function is only ever called on a column whose type is numeric.
        *   **`readCsvStreaming`:** Prove that the output DataFrame's schema *exactly* matches the type-level schema provided, preventing runtime errors from malformed CSVs.
    *   Refine our types to be more specific and expressive, allowing us to prove more complex properties.