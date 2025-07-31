# Sara Project: Naming Guidelines

This document outlines the naming conventions to be followed throughout the Sara Haskell project. Adhering to these guidelines ensures consistency, readability, and idiomatic Haskell style, making the codebase easier to understand, maintain, and contribute to.

These guidelines are based on standard Haskell practices and aim to promote clarity and predictability.

## 1. Modules

*   **Convention:** `CamelCase`, with each component capitalized. Use hierarchical naming to reflect the directory structure and logical grouping of functionality.
*   **Purpose:** Modules should clearly indicate their content and purpose.
*   **Examples:**
    *   `Sara.DataFrame.Types`
    *   `Sara.DataFrame.Wrangling`
    *   `Sara.DataFrame.Aggregate`
    *   `Sara.Error`

## 2. Types (Data Types, Type Synonyms, Type Families)

*   **Convention:** `CamelCase`, always starting with a capital letter.
*   **Purpose:** Distinguish types from values and functions.
*   **Examples:**
    *   `DataFrame`
    *   `DFValue`
    *   `Row`
    *   `SortCriterion`
    *   `SaraError`

## 3. Type Variables

*   **Convention:** Single lowercase letters (e.g., `a`, `b`, `c`) for generic types, or short `camelCase` (e.g., `cols`, `colName`, `schema`) for more descriptive type variables, especially in type-level programming contexts.
*   **Purpose:** Indicate parameters to polymorphic functions or data types.
*   **Examples:**
    *   `f :: a -> a`
    *   `DataFrame (cols :: [(Symbol, Type)])`
    *   `HasColumn (col :: Symbol) (schema :: [(Symbol, Type)])`

## 4. Functions and Values (Top-Level and Local Bindings)

*   **Convention:** `camelCase`, always starting with a lowercase letter.
*   **Purpose:** Standard convention for executable code.
*   **Examples:**
    *   `filterRows`
    *   `readCsv`
    *   `sumAgg`
    *   `createTestDataFrame`
    *   `myDataFrame`

## 5. Type Class Methods

*   **Convention:** `camelCase`, always starting with a lowercase letter.
*   **Purpose:** Follows the function naming convention.
*   **Examples:**
    *   `aggregate` (from `Aggregatable` type class)

## 6. Data Constructors

*   **Convention:** `CamelCase`, always starting with a capital letter.
*   **Purpose:** Distinguish constructors from functions and values.
*   **Examples:**
    *   `IntValue`
    *   `TextValue`
    *   `DoubleValue`
    *   `Ascending`
    *   `Descending`
    *   `InnerJoin`

## 7. Record Fields

*   **Convention:** `camelCase`, often prefixed with the type name to avoid clashes (e.g., `dfMap` for a field in `DataFrame`).
*   **Purpose:** Clearly associate fields with their parent type.
*   **Examples:**
    *   `scColumn` (for `SortCriterion` column)
    *   `scOrder` (for `SortCriterion` order)
    *   `dfMap` (for the internal map of a `DataFrame`)

## 8. Constants

*   **Convention:** `ALL_CAPS_WITH_UNDERSCORES`.
*   **Purpose:** Indicate values that are fixed and globally accessible.
*   **Examples:**
    *   `DEFAULT_WINDOW_SIZE` (if applicable)

## 9. Operators

*   **Convention:** Symbolic characters (e.g., `.` for composition, `$` for application, custom operators like `.+`, `.>`).
*   **Purpose:** Define custom infix operations. Ensure they are well-documented and intuitive.
*   **Examples:**
    *   `(.>)` (greater than for columns)
    *   `(.==)` (equality for columns)
    *   `(:*:)` (type-level list concatenation)

## 10. File Names

*   **Convention:** Match the module name, with a `.hs` extension.
*   **Purpose:** Direct mapping between file system and module structure.
*   **Examples:**
    *   `Types.hs` (for `Sara.DataFrame.Types`)
    *   `Wrangling.hs` (for `Sara.DataFrame.Wrangling`)

## 11. Test Files

*   **Convention:** Typically `Test*.hs` or `*Spec.hs`.
*   **Purpose:** Clearly identify test suites.
*   **Examples:**
    *   `TestSuite.hs`
    *   `TypeLevel.hs`

By adhering to these guidelines, we can ensure a consistent and high-quality codebase for the Sara project.
