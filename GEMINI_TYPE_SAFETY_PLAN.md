# Gemini Type Safety Plan for Sara

This document outlines the steps to systematically check and improve the type safety of the Sara project, aiming for 100% compile-time guarantees.

## Steps:

1.  **Deep Dive into Core Type Definitions (`Sara.DataFrame.Types.hs`):**
    *   Understand how the `DataFrame` type is parameterized by its schema (`[(Symbol, Type)]`).
    *   Analyze `TypeOf`, `HasColumn`, `HasColumns`, `Append`, `Remove`, `Nub`, `SelectCols`, `JoinCols`, `UpdateColumn`, etc. Verify their logic.
    *   Examine custom type classes and constraints (e.g., `KnownColumns`, `CanBeDFValue`).

2.  **Review Type-Level Logic in Functions:**
    *   For each function (e.g., `filterRows`, `sumAgg`, `mutate`, `filterByBoolColumn`), meticulously examine its type signature.
    *   Understand how GHC's type inference works with the type families and constraints.
    *   Observe how `Proxy` is used to pass type-level information.

3.  **"Should-Not-Compile" Tests:**
    *   Review existing tests in `test/ShouldNotCompile/` and `test/run-should-not-compile-tests.sh`.
    *   Ensure coverage for common invalid scenarios.
    *   Add new "should-not-compile" tests for new functionality or refactoring.

4.  **Comprehensive Unit and Integration Tests:**
    *   Ensure valid type-safe operations produce correct runtime results.
    *   Verify that the resulting DataFrame's schema matches the expected type-level schema.

5.  **GHC Warnings and Extensions:**
    *   Confirm strict GHC flags (e.g., `-Wall`, `-Werror`) are used.
    *   Verify appropriate GHC extensions are enabled.

6.  **Code Review and Documentation:**
    *   Suggest peer review for complex type-level code.
    *   Ensure design decisions for type families and constraints are well-documented.
