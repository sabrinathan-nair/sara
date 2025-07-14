# Potential Type Safety Improvements for Sara

This document outlines potential future improvements to enhance the type safety and robustness of the Sara DataFrame library.

## Phase 1: Core Type System Enhancements

1.  **Eliminating `fromDFValueUnsafe`:**
    *   **Current State:** The `fromDFValueUnsafe` function, despite its name, still represents a potential runtime error if a type mismatch or `NA` value occurs. While it has a `default` implementation that throws an error, its very existence means a path to runtime failure.
    *   **Potential Improvement:** Ideally, all conversions from `DFValue` to a concrete type `a` should be handled safely via `fromDFValue :: DFValue -> Maybe a`. Any code that *needs* to assert the presence and type of a value should do so with explicit pattern matching on `Maybe` or by using a type-level proof that guarantees the value's presence and type. This would push all such errors to compile-time. This might involve more complex type-level machinery to track the presence and type of values within `DFValue` at the type level.

2.  **Explicit Nullability Tracking in Schema:**
    *   **Current State:** The `NA` constructor in `DFValue` allows any column to potentially hold a missing value. While `isNA` helps, the schema `[(Symbol, Type)]` doesn't explicitly encode whether a column is nullable or not.
    *   **Potential Improvement:** The schema could be extended to `[(Symbol, Type, Nullability)]`, where `Nullability` is a type-level boolean (e.g., `IsNullable` or `NotNullable`). This would allow the type system to enforce that operations on non-nullable columns don't produce `NA`s, and that `NA`s are correctly handled when interacting with nullable columns. This is a significant change but would push more `NA` related errors to compile-time.

3.  **More Expressive Type-Level Row Representation:**
    *   **Current State:** `TypeLevelRow` is a `newtype` around `Map T.Text DFValue`. While it's associated with a type-level schema, the actual `Map` operations are still runtime.
    *   **Potential Improvement:** For ultimate type safety, one could explore using a truly type-level record system (e.g., libraries like `Data.Row` or `Vinyl`) where the presence and type of each field in a row are enforced at compile time, rather than relying on runtime `Map` lookups. This is a very advanced and often complex approach, but it would eliminate a class of runtime errors related to incorrect field access within a row.
