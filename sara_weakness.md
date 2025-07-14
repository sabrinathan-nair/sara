# Sara Project: Runtime Vulnerabilities and Proposed Solutions

This document outlines the remaining runtime vulnerabilities in the Sara DataFrame library, despite its strong type-level programming foundation. For each vulnerability, a proposed solution is offered, with the list ordered to prioritize "low-hanging fruit" – changes that offer a substantial boost to compile-time guarantees with relatively less effort.

## 1. Explicit `error` and `fail` Calls (High Priority / Low-Hanging Fruit)

**Description:** Direct calls to `error` or `fail` immediately crash the program at runtime when an unexpected condition or malformed data is encountered. This bypasses any graceful error handling.

**Locations:**
*   `src/Sara/DataFrame/IO.hs`:
    *   `readJSON`: JSON parsing errors, column count mismatches.
    *   `validateDFValue`: Type mismatches.
*   `src/Sara/DataFrame/SQL.hs`:
    *   `sqlDataToDFValue`: Type mismatches, unsupported SQL types (`SQLBlob`).
    *   `readSQL`: SQL query result column count mismatches, internal errors (e.g., column not found in expected names list), `rowData V.!? colIndex` `Nothing` case.
*   `src/Sara/DataFrame/Instances.hs`:
    *   `C.FromField` instances (e.g., `Bool`, `Day`, `UTCTime`, `DFValue`): Parsing failures.

**Proposed Solution:**
Replace all instances of `error` and `fail` with explicit error handling using `Either String a` or `Maybe a`. This shifts error handling from crashing the program to requiring callers to explicitly deal with potential failures. This is a mechanical but impactful change.

## 2. Partial Functions (`Map.!`) Without Compile-Time Guarantees (Medium Priority / Low-Hanging Fruit)

**Description:** Use of partial functions like `Map.!` (which crashes if a key is not found) in contexts where the key's existence is not strictly guaranteed by the type system.

**Locations:**
*   `src/Sara/DataFrame/Statistics.hs`: `rollingApply` (`dfMap Map.! (T.pack colName)`).
*   `src/Sara/DataFrame/Strings.hs`: `contains` (`dfMap Map.! colName`).

**Proposed Solution:**
Refactor these functions to accept `Proxy col` arguments and introduce `HasColumn col cols` constraints. This ensures that the column is guaranteed to exist in the DataFrame's schema at compile time, making the `Map.!` operation safe.

## 3. `fromJust` Usage (Medium Priority / Medium Effort)

**Description:** The use of `fromJust` assumes that a `Maybe` value will always be `Just`. If the `Maybe` value is `Nothing` at runtime (due to data inconsistencies not caught by the type system), `fromJust` will cause a runtime crash.

**Locations:**
*   `src/Sara/DataFrame/Expression.hs`: `evaluateExpr (Col p)` (`fromJust (fromDFValue (...))`).
*   `src/Sara/DataFrame/Transform.hs`: `applyColumn` (`fromJust (fromDFValue v)`).

**Proposed Solution:**
This is a symptom of the underlying `DFValue` representation and data ingestion issues.
*   **Short-term:** If the type system *truly* guarantees the `Just` case (which it should after previous refactorings), then `fromJust` is technically safe. However, if there's any doubt about runtime data integrity, consider propagating `Maybe` or `Either` from `fromDFValue` and handling it explicitly.
*   **Long-term:** A more fundamental redesign of `DFValue` and `DataFrame` (see point 6) would eliminate the need for `fromJust` by ensuring type-safe extraction.

## 4. Silent `NA` Propagation and Data Loss (Medium Priority / Medium Effort)

**Description:** Operations that silently insert `NA` values or drop data if a column is missing or a type doesn't match at runtime, leading to incorrect results rather than compile-time errors.

**Locations:**
*   `src/Sara/DataFrame/Aggregate.hs` (`sumAgg`, `meanAgg`, `countAgg`):
    *   `Map.lookup aggColName dfMap` returning `Nothing` (leading to `V.empty`).
    *   `V.catMaybes $ V.map (fromDFValue @a) col` (silently dropping values).
*   `src/Sara/DataFrame/Join.hs` (`joinDF`'s `processColumn` and join logic): `fromMaybe NA (Map.lookup colName r1)`.
*   `src/Sara/DataFrame/Missing.hs` (`fillna`, `ffill`, `bfill`): Operations on generic `DFValue`s that can lead to type inconsistencies.
*   `src/Sara/DataFrame/Strings.hs` (`applyTextTransform`, `applyTextPredicate`, `lower`, `upper`, `strip`, `contains`, `replace`): Handling of non-`TextValue`s.
*   `src/Sara/DataFrame/Transform.hs` (`melt`): `Map.lookup valVar row` and `Map.findWithDefault NA`.

**Proposed Solution:**
*   **For `Map.lookup`:** Where a column is *guaranteed* by `HasColumn`, ensure that the runtime `DataFrame` is consistent with the schema (e.g., by stricter data ingestion). If not guaranteed, explicitly handle the `Maybe` result.
*   **For `fromDFValue` and `DFValue` operations:**
    *   Introduce stronger type constraints to functions like `fillna`, `ffill`, `bfill`, and string operations to ensure the `DFValue` matches the expected type.
    *   Consider making `fromDFValue` return `Either String a` to force explicit error handling for type mismatches.
*   **For `melt`:** Add type-level constraints to `melt` to ensure `id_vars` and `value_vars` exist in the input schema.

## 5. Inconsistent Input Schema/Data for Operations (High Priority / High Effort)

**Description:** Operations that make implicit runtime assumptions about the consistency of input DataFrames (e.g., same columns, same row counts) without compile-time enforcement.

**Locations:**
*   `src/Sara/DataFrame/Concat.hs` (`concatDF`):
    *   `ConcatRows`: Assumes all input DataFrames have the same columns.
    *   `ConcatColumns`: Assumes same number of rows and handles type conflicts by overwriting.

**Proposed Solution:**
Introduce more sophisticated type-level constraints to enforce schema and row count consistency for input DataFrames. This would likely involve:
*   For `ConcatRows`: A type-level constraint ensuring `cols1 ~ cols2 ~ ... ~ colsN` for all input DataFrames.
*   For `ConcatColumns`: A type-level constraint ensuring all input DataFrames have the same number of rows (a very advanced type-level feature).
*   A type-safe union for `DataFrame`s that checks for type consistency of overlapping columns.

## 6. Fundamental `DataFrame` Representation and Data Ingestion (Long-Term / Major Effort)

**Description:** The deepest level of vulnerability stems from the `DataFrame` being a `newtype` around `Map T.Text Column` and `DFValue` being a sum type. This allows runtime inconsistencies that the type system cannot fully prevent. Data ingestion from external sources (CSV, JSON, SQL) is also a major source of runtime errors due to implicit type conversions and `fail`/`error` calls.

**Locations:**
*   Core `DataFrame` type definition (`src/Sara/DataFrame/Types.hs`).
*   `DFValue` type definition (`src/Sara/DataFrame/Types.hs`).
*   Data ingestion functions (`readCsv`, `readJSON` in `src/Sara/DataFrame/IO.hs`, `inferCsvSchema` in `src/Sara/DataFrame/Static.hs`, `readSQL` in `src/Sara/DataFrame/SQL.hs`).
*   `FromJSON DFValue` and `C.FromField DFValue` instances (`src/Sara/DataFrame/Types.hs`, `src/Sara/DataFrame/Instances.hs`).

**Proposed Solution:**
This requires a major architectural redesign:
*   **Redesign `DataFrame` Representation:** Move away from `Map T.Text Column` to a GADT that directly encodes the type-level schema into the data structure. This would eliminate runtime `Map.lookup` for column access and allow the compiler to guarantee the type of values within a column.
*   **Type-Safe Row Parsing:** Implement robust, type-safe parsing for external data sources that returns `Either` on failure, forcing explicit error handling at the boundary of the type-safe core. This would involve generating custom `FromField`/`FromJSON` instances based on the inferred schema, or using a more advanced parsing library.
*   **Refine `CanBeDFValue`:** Potentially make `fromDFValue` return `a` directly under stronger type-level constraints, or introduce a new type class for guaranteed conversions.

These changes would provide the most complete compile-time guarantees but represent a significant undertaking.
