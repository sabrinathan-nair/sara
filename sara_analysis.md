# Sara Project: Detailed Analysis (Current State)

## 1. Introduction

The Sara project aims to provide a type-safe DataFrame library in Haskell, leveraging advanced type-level programming to ensure data integrity and prevent common runtime errors associated with data manipulation. The core idea is to represent DataFrame schemas at the type level, allowing the compiler to verify operations against these schemas.

## 2. Architectural Overview

At its heart, the Sara DataFrame library is built upon:

*   **`DataFrame (cols :: [(Symbol, Type)])`**: A `newtype` wrapper around `Map T.Text Column`. The `cols` type parameter is a type-level list of `(Symbol, Type)` tuples, representing the column names and their corresponding Haskell types (e.g., `'['("name", T.Text), '("age", Int)]`). This is where the compile-time schema is defined.
*   **`DFValue`**: A GADT (Generalized Algebraic Data Type) that serves as the runtime container for various data types (e.g., `IntValue`, `DoubleValue`, `TextValue`, `BoolValue`, `NA`). All data within a `DataFrame` is ultimately stored as `DFValue`s.
*   **`Column`**: A `Vector DFValue`, representing a single column of data.
*   **`Row`**: A `Map T.Text DFValue`, representing a single row of data at runtime.
*   **Type Families**: Extensive use of type families (e.g., `HasColumn`, `TypeOf`, `JoinCols`, `UpdateColumn`, `Append`, `Remove`, `Nub`) to manipulate and reason about DataFrame schemas at the type level.
*   **Template Haskell**: Used in `Sara.DataFrame.Static` to infer DataFrame schemas from CSV files at compile time, generating type-level schema definitions.
*   **`Expr` GADT**: A type-safe GADT for building expressions that operate on DataFrame columns, ensuring that column references and types within expressions are validated at compile time.

## 3. Strengths (What's Working Well for Type Safety)

*   **Strong Type-Level Schema Enforcement:** The project excels at defining and enforcing DataFrame schemas at compile time. Operations like `selectColumns`, `dropColumns`, and schema transformations (`Append`, `Remove`, `Nub`, `UpdateColumn`) are highly type-safe, preventing errors related to non-existent columns or incorrect schema manipulations.
*   **Type-Safe Expression (`Expr`) GADT:** The `Expr` GADT, coupled with constraints like `HasColumn` and `TypeOf`, ensures that expressions built to operate on DataFrame columns are type-checked. This prevents operations on non-existent columns or type-mismatched data within expressions.
*   **Improved Core Operations:** Recent refactorings of `filterRows`, `applyColumn`, `addColumn`, and `mutate` have significantly enhanced their compile-time safety. These functions now leverage the type system to guarantee that column access and type conversions are valid, reducing reliance on runtime `Maybe` handling for expected success paths.
*   **Clear Separation of Concerns:** The codebase is modular, with distinct modules for types, I/O, transformations, aggregations, etc., which aids in maintainability and understanding.
*   **Compile-Time Schema Inference:** The use of Template Haskell to infer schemas from CSV files is a powerful feature, bridging the gap between untyped data sources and type-safe DataFrame operations.

## 4. Weaknesses (Remaining Runtime Vulnerabilities)

Despite its strong type-level foundation, the project is not yet 100% compile-time safe. The primary challenges arise at the boundary between the compile-time type system and the runtime data representation, particularly during data ingestion and when operating on the generic `DFValue` type.

*   **Impedance Mismatch (Type-Level vs. Runtime Data):** The `DataFrame` is a `newtype` around `Map T.Text Column`, and `Row` is `Map T.Text DFValue`. While the type system enforces the *declared* schema, it cannot prevent a *runtime instance* of a `DataFrame` or `Row` from being inconsistent with that schema (e.g., a column missing from the `Map`, or a `DFValue` containing a type different from its declared type). This leads to runtime checks and potential failures.

*   **Data Ingestion Vulnerabilities:**
    *   **Explicit `error` and `fail` Calls:** Functions in `src/Sara/DataFrame/IO.hs`, `src/Sara/DataFrame/SQL.hs`, and `src/Sara/DataFrame/Instances.hs` (especially `FromField` instances for CSV parsing) frequently use `error` or `fail`. If external data does not conform to expectations (e.g., malformed JSON, incorrect CSV types, SQL query mismatches), the program will crash at runtime.
    *   **Heuristic Type Inference:** In `src/Sara/DataFrame/Static.hs`, `inferDFType` uses runtime heuristics to guess column types from sample data. If the inferred type is incorrect for the actual data, it can lead to runtime type mismatches later.
    *   **Implicit Type Coercion/Fallback:** The `C.FromField DFValue` instance in `src/Sara/DataFrame/Instances.hs` falls back to `TextValue` if a field cannot be parsed as other types. This can silently introduce `TextValue`s into columns declared as numeric, leading to runtime errors when numeric operations are attempted.

*   **`fromJust` Usage:** The use of `fromJust` (e.g., in `evaluateExpr` in `src/Sara/DataFrame/Expression.hs` and `applyColumn` in `src/Sara/DataFrame/Transform.hs`) is a direct runtime crash point. If `fromDFValue` returns `Nothing` (due to a `DFValue` not matching the expected type), `fromJust` will cause the program to terminate.

*   **Silent `NA` Propagation and Data Loss:** Several functions (e.g., in `src/Sara/DataFrame/Aggregate.hs`, `src/Sara/DataFrame/Missing.hs`, `src/Sara/DataFrame/Strings.hs`, `src/Sara/DataFrame/Concat.hs`) can silently insert `NA` values or drop data if a column is missing at runtime or a type doesn't match. This leads to incorrect results rather than compile-time errors.

*   **Partial Functions (`Map.!`):** While reduced, some modules still use `Map.!` (e.g., `rollingApply` in `src/Sara/DataFrame/Statistics.hs`, `contains` in `src/Sara/DataFrame/Strings.hs`). If the key is not present in the runtime `Map`, these operations will crash.

*   **Inconsistent Input Schema/Data for Operations:** Functions like `concatDF` in `src/Sara/DataFrame/Concat.hs` make implicit runtime assumptions about the consistency of input DataFrames (e.g., all DataFrames having the same columns for row-wise concatenation, or the same number of rows for column-wise concatenation). Violations of these assumptions lead to silent data loss, malformed DataFrames, or runtime errors.

## 5. Overall Assessment

The Sara project has a robust and well-designed type-level foundation that provides significant compile-time guarantees for DataFrame schema manipulation and expression building. The recent refactorings have further strengthened these guarantees for core operations.

However, the project is not yet 100% compile-time safe. The primary remaining challenges lie at the interface between the compile-time type system and the runtime data. The generic `DFValue` representation, coupled with the current data ingestion mechanisms, allows for runtime inconsistencies that can lead to crashes (`error`, `fail`, `fromJust`) or silent data corruption (`NA` propagation, implicit type coercion).

Achieving true 100% compile-time guarantees would necessitate a more fundamental redesign of the `DataFrame`'s internal data representation to eliminate runtime `Map` lookups and `DFValue` type checks, along with a complete overhaul of data ingestion to enforce strict type validation at the earliest possible stage.
