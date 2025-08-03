# Pending Unit Test Coverage for Sara.DataFrame Modules

This document lists modules within the `src/Sara/DataFrame` directory for which unit test coverage needs to be expanded.

## Modules with Pending Unit Test Expansion:

*   **`Sara.DataFrame.Aggregate.hs`**: While some aggregation functions are covered by QuickCheck, dedicated unit tests for edge cases (e.g., empty groups, all NA values in a group, single-element groups) are needed.
*   **`Sara.DataFrame.Apply.hs`**: This module likely contains functions for applying arbitrary functions to DataFrame columns. Unit tests are needed to cover various function types, error handling (e.g., type mismatches), and performance considerations.
*   **`Sara.DataFrame.CsvInstances.hs`**: This module likely defines instances for CSV parsing. Unit tests should cover various CSV formats, malformed data, and edge cases for parsing.
*   **`Sara.DataFrame.Internal.hs`**: This module contains internal helper functions. Unit tests should be written for any exposed or critical internal functions.
*   **`Sara.DataFrame.IO.hs`**: While streaming tests exist, comprehensive unit tests for all I/O functions (e.g., `readCsv`, `readJSON`, `writeCsv`, `writeJSON`) covering file existence, permissions, malformed files, and large file handling are needed.
*   **`Sara.DataFrame.Join.hs`**: While a basic join test exists, more comprehensive unit tests for different join types (inner, left, right, outer), edge cases (empty DataFrames, no matching keys, duplicate keys), and performance are needed.
*   **`Sara.DataFrame.Predicate.hs`**: This module defines predicates for filtering. Unit tests should cover all predicate types, combinations of predicates, and edge cases (e.g., predicates that always return true/false, predicates with NA values).
*   **`Sara.DataFrame.Static.hs`**: This module likely deals with static (compile-time) DataFrame operations. Unit tests should verify the correctness of type-level manipulations and any associated runtime functions.
*   **`Sara.DataFrame.Statistics.hs`**: This module likely contains statistical functions. Unit tests should cover various statistical measures, edge cases (e.g., empty data, single-element data, all identical values), and numerical stability.
*   **`Sara.DataFrame.Strings.hs`**: This module likely contains string manipulation functions. Unit tests should cover various string operations, edge cases (e.g., empty strings, special characters), and encoding issues.
*   **`Sara.DataFrame.TimeSeries.hs`**: This module likely contains time series specific operations. Unit tests should cover various time series functions, edge cases (e.g., irregular time series, missing timestamps), and time zone handling.
*   **`Sara.DataFrame.Transform.hs`**: While `mutate` and `applyColumn` have some tests, more comprehensive unit tests for all transformation functions, including edge cases and error handling, are needed.
*   **`Sara.DataFrame.Types.hs`**: This module defines core DataFrame types and type classes. Unit tests should cover the correctness of `DFValue` conversions, `DataFrame` construction, and any helper functions.
*   **`Sara.DataFrame.Wrangling.hs`**: While `selectColumns`, `dropColumns`, `filterRows`, and `sortDataFrame` have some tests, more comprehensive unit tests for all wrangling functions, including edge cases and error handling, are needed.
