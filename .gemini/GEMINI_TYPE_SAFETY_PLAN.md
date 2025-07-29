# Sara Type Safety Refactoring Plan

This document outlines the plan to refactor the Sara library to be 100% type-safe, with all potential runtime errors moved to compile-time.

## Key Areas for Improvement

1.  **Inconsistent Type Safety:** The project currently has a mix of type-safe and runtime-reliant functions. The goal is to make all functions fully type-safe.
2.  **`DFValue` and `Row` Types:** The `DFValue` and `Row` types are the primary sources of runtime uncertainty. We need to minimize our reliance on these types and instead use type-level representations of the schema as much as possible.

## Refactoring Plan

The following is a summary of the refactoring that has been completed:

*   **`fromDFValueUnsafe`:** All uses of `fromDFValueUnsafe` have been replaced with safe alternatives.
*   **`applyColumn`:** `applyColumn` has been rewritten to be fully type-safe.
*   **`filterRows`:** `filterRows` has been refactored to use a more type-safe `Predicate`.
*   **Aggregation Functions:** The aggregation functions have been rewritten to be fully type-safe.
*   **`joinDF`:** `joinDF` has been rewritten to be fully type-safe.
*   **`readCsv` and `readJSON`:** These functions have been improved to provide stronger compile-time guarantees.