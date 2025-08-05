# Modules Requiring Unit Test Expansion

This document lists modules in the Sara project that require further unit test coverage, focusing on edge cases, invalid inputs, and error paths as outlined in `final_tests.md`.

## Modules with Insufficient or Missing Unit Tests:

- `Sara.REPL.hs`: Needs comprehensive tests for command parsing, execution, and output.
- `Sara.Schema.Definitions.hs`: Needs tests for schema creation, validation, and manipulation.
- `Sara.DataFrame.Internal.hs`: Needs tests for its core internal helper functionalities.
- `Sara.DataFrame.Static.hs`: Needs tests for its static DataFrame operations.
- `Sara.DataFrame.Statistics.hs`: Needs tests for various statistical calculations, including edge cases (empty data, identical values, NA values).
- `Sara.DataFrame.Strings.hs`: Needs tests for string manipulation functions, including edge cases (empty strings, special characters).
- `Sara.DataFrame.TimeSeries.hs`: Needs tests for time series operations, including edge cases (empty series, single-point series, irregular intervals).
- `Sara.DataFrame.Types.hs`: Needs tests for type conversions, `DFValue` handling, and DataFrame creation from rows/columns.
