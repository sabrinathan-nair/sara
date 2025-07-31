# Sara Project Progress Report

## Analysis of Sara Project

The Sara project aims to provide a type-safe and performant DataFrame library in Haskell, leveraging type-level programming for compile-time guarantees. The core `DataFrame` type is parameterized by its column names, enabling strong type safety for schema-related operations. Existing functions like `selectColumns` and `dropColumns` successfully demonstrate this type-safe approach by manipulating the schema at the type level.

However, the implementation has been inconsistent. Some functions, particularly those involving row-wise operations or data ingestion, have historically fallen back to runtime checks using `Maybe` or `DFValue` types. This can lead to runtime errors, undermining the goal of compile-time safety.

The project is currently undergoing a refactoring effort to align all DataFrame operations with the type-safe patterns, ensuring that functions return `Either SaraError` for explicit error handling instead of relying on `Maybe` or partial functions like `fromJust`/`fromMaybe`. Performance improvements through streaming are also a key focus, with the `streaming` library integrated. Property-based testing with QuickCheck is also being utilized to enhance test coverage and ensure correctness.

## Work Done So Far

### Error Handling Refactoring:
*   **`readJSONStreaming` in `src/Sara/DataFrame/IO.hs`**: Refactored to return `IO (Either SaraError (Stream (Of (DataFrame cols)) IO ()))`, providing explicit error handling for JSON parsing and header mismatches.
*   **`readCsvStreaming` in `src/Sara/DataFrame/IO.hs`**: Refactored to return `IO (Either SaraError (Stream (Of (DataFrame cols)) IO ()))`, ensuring explicit error handling for CSV parsing and header mismatches.
*   **`applyColumn` in `src/Sara/DataFrame/Transform.hs`**: Modified to return `Stream (Of (Either SaraError (DataFrame newCols))) IO ()`, wrapping the result in `Either` for error propagation.
*   **`joinDF` in `src/Sara/DataFrame/Join.hs`**: Modified to return `Stream (Of (Either SaraError (DataFrame colsOut))) IO ()`, enabling error handling within the join operation.

### Codebase Cleanup and Integration:
*   **Import Fixes**: Addressed missing imports (`SaraError`, `fromJust`, `partitionEithers`, `liftIO`) and removed redundant imports across `src/Sara/DataFrame/Wrangling.hs`, `src/Sara/DataFrame/Transform.hs`, `src/Sara/DataFrame/Join.hs`, and `test/TestSuite.hs`.
*   **`app/Tutorial.hs` Update**: Updated the tutorial application to correctly handle the new `Either SaraError` return types from `readCsvStreaming` and `filterByBoolColumn`.
*   **`test/TestSuite.hs` Adjustments**: Modified test cases to correctly handle `Either SaraError` in streams and fixed `expectationFailure` calls to return appropriate dummy values in `IO` contexts.

## Pending Work

### Immediate Blocker:
*   **`streamToDf` Function in `test/TestSuite.hs`**: This helper function in the test suite is still causing compilation errors related to type mismatches with `S.toList` and `partitionEithers`. This needs to be fixed to allow the test suite to compile and run.

### Error Handling Consistency (Ongoing):
*   **Refactor Remaining `Maybe` and Partial Functions**: Conduct a comprehensive scan of the codebase to identify any other functions that currently return `Maybe` or use partial functions like `fromJust`/`fromMaybe` for error handling. These should be refactored to return `Either SaraError` for consistent and robust error management.

### Streaming Integration (Verification and Completion):
*   **Aggregation Functions (`sumAgg`, `meanAgg`, `countAgg`)**: Verify that these functions are fully integrated with the `streaming` library and correctly handle errors through the `Either SaraError` type.
*   **Row-wise Operations (`filterByBoolColumn`)**: Confirm that `filterByBoolColumn` is fully streaming and error-aware.
*   **Other DataFrame Operations (`sortDataFrame`)**: Integrate streaming and error handling into `sortDataFrame` and any other relevant DataFrame operations that currently operate on in-memory DataFrames.

### Testing Enhancements:
*   **QuickCheck Integration**: Ensure that QuickCheck is fully integrated and that property-based tests are comprehensive for all critical DataFrame operations, especially after streaming and error handling refactoring.

### Advanced Type Safety:
*   **Liquid Haskell Investigation**: Research and investigate the potential of using Liquid Haskell for refinement types to further enhance compile-time guarantees and prevent a broader class of runtime errors. This is a longer-term goal but should be kept in mind.

This granular breakdown should provide a clear roadmap for continuing the development of the Sara project.
