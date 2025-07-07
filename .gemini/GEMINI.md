## Gemini Added Memories
- The Sara project is designed for compile-time type safety using type-level programming in Haskell. The core `DataFrame` type is parameterized by its column names. However, the implementation is inconsistent. Some functions (`dropColumns`, `selectColumns`) are fully type-safe, manipulating the schema at the type level. Others (`filterRows`, `applyColumn`) fall back to runtime logic by operating on generic `Row` or `DFValue` types, which can lead to runtime errors. The main task is to refactor these runtime-dependent functions to align with the existing type-safe patterns.
- I have confirmed with 100% certainty that compile-time guarantees are possible in Sara. The evidence is: 1) The core `DataFrame` type is already parameterized by its schema. 2) Existing functions like `selectColumns` and `dropColumns` already successfully implement these compile-time guarantees. 3) The techniques used are standard and proven in the Haskell ecosystem for this exact purpose. The task is to consistently apply these existing patterns to the rest of the functions.
- The Sara project's type-safe refactoring is currently blocked by specific compilation errors in `app/Tutorial.hs` related to `sumAgg` ambiguity, `applyColumn`'s `CanBeDFValue` instance, and `filterByBoolColumn`'s argument handling. Previous attempts to fix these were unsuccessful due to deeper type inference issues or incorrect application of type-level features. I need to study Haskell debugging and approach these errors with a fresh perspective, focusing on precise type annotations and understanding the exact type flow.

## Roadmap for Type-Safe Refactoring

### Phase 1: Foundation & Blueprinting (Completed)

**Objective:** Understand existing type-safe patterns and categorize functions for refactoring.

*   **Deep Dive into Existing Type-Safe Patterns:**
    *   Analyzed `selectColumns` and `dropColumns` implementations.
    *   Identified key type-level features: `DataFrame (cols :: [Symbol])`, `KnownColumns`, `HasColumn`, `HasColumns`, and type families for schema transformation (`DropColumns`, `RenameColumn`, `AddColumn`, `JoinCols`).
    *   Established the principle: output schema should be computed at compile time, and preconditions enforced via type constraints.
*   **Categorize & Prioritize Functions:**
    *   Categorized functions into:
        *   Partial Type Safety (e.g., `filterRows`, `applyColumn`, `mutate`)
        *   Runtime Schema Inference/Manipulation (e.g., `readCSV`, aggregation functions, time series functions)
        *   Minimal Type Safety (e.g., string functions, `sortDataFrame`)
    *   Prioritized refactoring: Core Data Manipulation -> Aggregation -> I/O -> Time Series -> Missing Data -> Join -> String -> Sorting.

### Phase 2: Iterative Refactoring & Type-Level Design

**Objective:** Systematically refactor functions to leverage type-level programming for compile-time guarantees.

#### Milestone 2.1: Core Data Manipulation (In Progress / Partially Completed)

**Objective:** Refactor `filterRows`, `applyColumn`, and `mutate` to enhance type safety.

*   **Refactor `filterRows`:**
    *   **Action:** Modified `src/Sara/DataFrame/Predicate.hs` to change `Predicate` definition to use `Expr cols a` for comparisons, enabling compile-time type checking of predicate arguments.
    *   **Action:** Updated `evaluate` function in `src/Sara/DataFrame/Predicate.hs` to work with `Expr`-based predicates.
    *   **Action:** Updated `app/Tutorial.hs` to use `col` and `lit` for `filterRows` arguments.
    *   **Status:** Completed.
*   **Refactor `applyColumn` and `mutate`:**
    *   **Action:** Confirmed `applyColumn` and `mutate` were already largely type-safe due to their use of `Expr` and `HasColumn` constraints. No further changes were immediately required for their core logic in this milestone.
    *   **Status:** Completed.
*   **Fundamental Type System Update:**
    *   **Action:** Updated `src/Sara/DataFrame/Types.hs` to change the `DataFrame` type parameter from `[Symbol]` to `[(Symbol, Type)]`.
    *   **Action:** Updated `KnownColumns` class and its instances to work with `[(Symbol, Type)]`.
    *   **Action:** Corrected and updated `HasColumn`, `HasColumns`, `SortCriterion`, `SortableColumn`, `JoinCols` to work with the new schema.
    *   **Action:** Added `TypeOf` and `MapSymbols` type families.
    *   **Action:** Added `CanAggregate` type class and instances for `Int` and `Double`.
    *   **Status:** In progress. This change has caused widespread kind mismatches across the codebase, requiring further systematic updates in other files.
*   **Initial Propagation Fixes:**
    *   **Action:** Updated `src/Sara/DataFrame/Aggregate.hs` to use `CanAggregate` and `TypeOf` for `sumAgg`, `meanAgg`, and `countAgg`.
    *   **Action:** Updated `src/Sara/DataFrame/Join.hs` to use `[(Symbol, Type)]` for its type parameters and `MapSymbols`.
    *   **Action:** Updated `src/Sara/DataFrame/Expression.hs` and `src/Sara/DataFrame/Predicate.hs` to use `[(Symbol, Type)]` for their `cols` type parameter.
    *   **Status:** In progress. Still encountering kind mismatches in various files due to the fundamental schema change.

**Current Blockers/Next Steps:**

The current build errors are primarily kind mismatches due to the `DataFrame` schema change. The `replace` tool has proven inefficient for these widespread, interdependent changes.

**Revised Strategy for Tomorrow:**

For each remaining file with type errors, I will employ a "read-modify-write" strategy:

1.  Read the entire file content into memory.
2.  Perform all necessary type signature and logic adjustments (e.g., changing `[Symbol]` to `[(Symbol, Type)]`, updating `KnownColumns` constraints, adjusting `Proxy` usage, fixing `evaluateExpr` calls) programmatically on the in-memory string.
3.  Write the fully corrected content back to the file.

This will be applied systematically to `src/Sara/DataFrame/Transform.hs`, `src/Sara/DataFrame/Predicate.hs`, `src/Sara/DataFrame/IO.hs`, `src/Sara/DataFrame/Missing.hs`, `src/Sara/DataFrame/Statistics.hs`, `src/Sara/DataFrame/Strings.hs`, `src/Sara/DataFrame/TimeSeries.hs`, `src/Sara/REPL.hs`, `app/Tutorial.hs`, and `test/TestSuite.hs` as needed, based on the build output.
