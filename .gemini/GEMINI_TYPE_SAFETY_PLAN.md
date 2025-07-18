
# Sara Type Safety Refactoring Plan

This document outlines the plan to refactor the Sara library to be 100% type-safe, with all potential runtime errors moved to compile-time.

## Key Areas for Improvement

1.  **Inconsistent Type Safety:** The project currently has a mix of type-safe and runtime-reliant functions. The goal is to make all functions fully type-safe.
2.  **`DFValue` and `Row` Types:** The `DFValue` and `Row` types are the primary sources of runtime uncertainty. We need to minimize our reliance on these types and instead use type-level representations of the schema as much as possible.
3.  **Problematic Functions:** Several functions have been identified as being particularly problematic, including `filterRows`, `applyColumn`, aggregation functions, `joinDF`, and the CSV/JSON reading functions.

## Refactoring Plan

The following is a step-by-step plan to refactor Sara for 100% type safety:

1.  **Eliminate `fromDFValueUnsafe`:**
    *   [x] Replace all uses of `fromDFValueUnsafe` with safe alternatives that use `fromDFValue` and handle the `Maybe` case explicitly.

2.  **Refactor `applyColumn`:**
    *   [x] Rewrite `applyColumn` to be fully type-safe, using a type class to constrain the types of the input and output columns and type families to compute the new schema.

3.  **Refactor `filterRows`:**
    *   [x] Refactor `filterRows` to use a more type-safe `Predicate`. This will likely involve using a GADT that is parameterized by the types of the columns being compared.

4.  **Refactor Aggregation Functions:**
    *   [x] Rewrite the aggregation functions (`sumAgg`, `meanAgg`, `countAgg`) to be fully type-safe, using a type class to constrain the types of the columns that can be aggregated and type families to compute the type of the resulting aggregated column.

5.  **Refactor `joinDF`:**
    *   [x] Rewrite `joinDF` to be fully type-safe, using type families to compute the schema of the joined `DataFrame` and to ensure that the join columns have compatible types.

6.  **Improve `readCsv` and `readJSON`:**
    *   [x] Use Template Haskell to generate parsing code that is specific to the expected schema, providing stronger compile-time guarantees and reducing the amount of runtime validation required.
