## Shortcomings in Sara (Areas for Future Work) Compared to Haskell's Frames Library

This document outlines potential areas for improvement in the Sara DataFrame library, drawing comparisons with the features and design patterns found in the `Frames` library.

### 1. Runtime Validation in Data Loading (`readJSON`, `validateDFValue`):
*   **Current State:** While `inferCsvSchema` provides strong compile-time guarantees for CSVs, `readJSON` and the internal `validateDFValue` still perform runtime type checking and can throw runtime errors if the JSON data doesn't conform to the expected schema.
*   **`Frames` Approach:** `Frames` often generates types directly from the data source (e.g., JSON schema inference), aiming to shift more of this validation to compile-time or provide more robust error handling during parsing.
*   **Potential Improvement:** Explore ways to make `readJSON` more robust, perhaps by returning `Either` for parsing errors or by integrating with a JSON schema validation library that can be used to generate type-level schemas.

### 2. Ergonomics and Type-Safety of Column Access/Manipulation (Beyond `Expr`):
*   **Current State:** While `Expr` and `mutate` provide type-safe ways to define transformations, direct column access (e.g., `dfMap Map.! colName`) still relies on runtime map lookups. Although the `HasColumn` constraint ensures existence, the ergonomics could be improved.
*   **`Frames` Approach:** `Frames` heavily utilizes lenses (often via `vinyl`) for concise, type-safe, and composable access and modification of individual columns within records. This allows for very fluid data manipulation with strong compile-time checks.
*   **Potential Improvement:** Investigate integrating a lens library (like `lens` or `optics`) or a type-level record library (like `vinyl` or `Data.Row`) to provide more ergonomic and deeply type-safe ways to access, modify, and project columns. This would likely involve a significant refactoring of the internal `DataFrame` representation.

### 3. Limited Type-Level Record Manipulation Primitives:
*   **Current State:** Sara has basic type-level list operations like `Append`, `Remove`, and `Nub` for schema manipulation.
*   **`Frames` Approach:** Libraries like `vinyl` (often used by `Frames`) provide a richer set of type-level primitives for manipulating heterogeneous lists and records, enabling more complex and flexible schema transformations (e.g., reordering columns, zipping records, splitting records) entirely at the type level.
*   **Potential Improvement:** Explore adopting or implementing more advanced type-level record manipulation primitives to enhance the flexibility and type safety of schema transformations.

### 4. Integration with Streaming for Large Datasets:
*   **Current State:** Sara's `toRows` and `fromRows` functions convert the entire DataFrame to a list of rows, which can be memory-intensive for very large datasets.
*   **`Frames` Approach:** `Frames` often integrates with streaming libraries (like `pipes` or `conduit`) to process data efficiently, allowing for operations on datasets that don't fit entirely in memory.
*   **Potential Improvement:** Investigate integrating a streaming library to enable more memory-efficient processing of large DataFrames, especially for I/O operations and transformations.

### 5. More Granular and Actionable Error Messages (Ongoing Refinement):
*   **Current State:** We've significantly improved `TypeError` messages, making them more informative than default GHC errors.
*   **`Frames` Approach:** Due to its deeper integration with type-level record systems, `Frames` can sometimes provide even more precise and context-aware error messages, guiding the user directly to the source of the type mismatch.
*   **Potential Improvement:** This is an ongoing effort. As the library evolves, continue to refine `TypeError` messages, especially for complex interactions between type families, to be as user-friendly and actionable as possible. This might involve more sophisticated type-level error reporting techniques.

### 6. Extensibility for Custom `DFValue` Types:
*   **Current State:** While `CanBeDFValue` allows for custom types, the `DFValue` ADT itself is closed. Adding new primitive types requires modifying `DFValue` and all its instances.
*   **`Frames` Approach:** Some type-safe data libraries might offer more open-ended ways to extend the set of supported types without modifying core data structures, perhaps through type families or plugin architectures.
*   **Potential Improvement:** Consider if there's a more extensible way to allow users to define and integrate their own primitive types into the `DFValue` system without requiring changes to the core library.
