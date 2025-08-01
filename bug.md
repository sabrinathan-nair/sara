# Bug Report: CSV Parsing Error in Sara Tutorial

## Problem Description
The `sara-tutorial` executable fails to run due to a CSV parsing error: `conversion error: no field named "employeesEmployeeID"`. This occurs when `readCsvStreaming` attempts to read `employees.csv`. The root cause is a mismatch between the column names expected by the `FromNamedRecord` instance (which are prefixed, e.g., `employeesEmployeeID`) and the actual column names in the CSV file (which are unprefixed, e.g., `EmployeeID`). The `inferCsvSchema` Template Haskell function generates record types with prefixed field names, leading to this discrepancy.

## Solutions Attempted and Outcomes

1.  **Initial Hypothesis: Prevent Prefixing in `inferCsvSchema`**
    *   **Attempt:** Modified `app/Tutorial.hs` to pass an empty string to `inferCsvSchema` (e.g., `$(inferCsvSchema "" "employees.csv")`) hoping it would prevent prefixing.
    *   **Outcome:** Failed with a compiler error (`Illegal type constructor or class name: ‘’`). An empty string is not a valid type name.

2.  **Refined Hypothesis: Handle Empty TypeName in `inferCsvSchema`**
    *   **Attempt:** Modified `inferCsvSchema` in `src/Sara/DataFrame/Static.hs` to use a default type name derived from the file path if an empty string was provided.
    *   **Outcome:** Failed with a compiler error (`Not in scope: type constructor or class ‘Sara.DataFrame.Internal.HasTypeName’`). This revealed a deeper issue with the `HasTypeName` typeclass not being correctly in scope.

3.  **Addressing `HasTypeName` Scope Issues (Multiple Attempts)**
    *   **Attempt 1 (Add `HasTypeName` typeclass and instance generation):** Added `HasTypeName` typeclass to `src/Sara/DataFrame/Internal.hs` and modified `inferCsvSchema` to generate instances.
    *   **Outcome:** Led to various compilation errors related to `HasTypeName` not being in scope or `getTypeName` not being a visible method, indicating module export issues or incorrect Template Haskell usage.

    *   **Attempt 2 (Manual `HasTypeName` instances in `app/Tutorial.hs`):** Temporarily added manual `HasTypeName` instances for `EmployeesRecord` and `DepartmentsRecord` directly in `app/Tutorial.hs` to bypass Template Haskell export problems.
    *   **Outcome:** Failed with parse errors due to `import` statements being out of order relative to the `class` and `instance` declarations.

    *   **Attempt 3 (Reorder imports/instances in `app/Tutorial.hs`):** Reordered the code in `app/Tutorial.hs` to place all imports first, then class/instance declarations.
    *   **Outcome:** Still failed with `HasTypeName` not in scope, suggesting the Template Haskell generated instances were not being picked up correctly or the manual instances were still problematic.

    *   **Attempt 4 (Re-export `HasTypeName` from `Sara.DataFrame.Internal`):** Modified `src/Sara/DataFrame/Internal.hs` to explicitly export `HasTypeName` and its methods.
    *   **Outcome:** Continued to face scope errors, indicating persistent issues with Haskell's module system and Template Haskell interactions.

4.  **Reverting and Restarting (Multiple Times)**
    *   **Attempt:** Undid all changes to the codebase to return to a clean state and re-evaluated the original error.
    *   **Outcome:** Confirmed the original parsing error persisted.

5.  **Focusing on `readCsvStreaming` and Prefixed Names**
    *   **Attempt:** Modified `app/Tutorial.hs` to use `Proxy @EmployeesRecord` and `col (Proxy @"EmployeesSalary")` in the `mutate` function, acknowledging the prefixed names.
    *   **Outcome:** Still resulted in the parsing error, confirming that `readCsvStreaming` was the bottleneck.

    *   **Attempt:** Modified `readCsvStreaming` in `src/Sara/DataFrame/IO.hs` to explicitly prefix the CSV headers with the `typeName` (obtained via `HasTypeName`) before passing to `C.decodeByName`.
    *   **Outcome:** Still resulted in the parsing error, indicating that the manual header manipulation was not correctly aligning with `C.decodeByName`'s expectations.

6.  **Current Approach: Manual `FromNamedRecord` Instance Generation**
    *   **Attempt:** Modified `inferCsvSchema` in `src/Sara/DataFrame/Static.hs` to manually construct the `FromNamedRecord` instance, explicitly mapping unprefixed CSV headers to prefixed Haskell record fields. This involved removing `DeriveAnyClass` for `FromNamedRecord` and writing custom Template Haskell code for `parseNamedRecord`.
    *   **Outcome:** This approach has led to a series of Template Haskell syntax errors (`parse error on input '$'`, `Couldn't match expected type: Q Clause`, etc.) due to the complexity of generating correct TH expressions for `bindS`, `fieldExp`, and `recConE`. We are currently stuck on resolving these syntax issues within the Template Haskell quotation.

## Next Steps
The current focus is on correctly implementing the manual `FromNamedRecord` instance generation in `src/Sara/DataFrame/Static.hs` to ensure the CSV parsing aligns with the prefixed record field names. Once this is resolved, we will re-verify the entire flow.
