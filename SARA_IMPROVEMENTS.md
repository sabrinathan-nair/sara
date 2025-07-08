
# Comparison with Haskell Frames and Areas for Improvement in Sara

## 1. Schema Definition and Inference

**Frames:**
The Frames library uses Template Haskell to infer the schema directly from a CSV file at compile-time. This is a major advantage as it automates the process of defining the data types that correspond to the columns in the data. This approach ensures that the types are always in sync with the data file, reducing the chance of runtime errors due to schema mismatches.

**Sara:**
Currently, Sara requires the user to manually define the schema at the type level (e.g., `DataFrame '[ '("name", String), '("age", Int)]`). While this provides explicitness, it has a few drawbacks:
- It is verbose and can be tedious for datasets with many columns.
- It's prone to errors if the schema of the data file changes; the user must manually update the type definition in the code.

**Recommendation:**
Sara should explore implementing a mechanism for schema inference. While Template Haskell is one way to do this, other approaches could be considered. For example, a function could read the header of a CSV and generate the necessary type-level information, even if it's not as fully automated as Template Haskell. This would significantly improve the usability of the library.

## 2. Consistent Type Safety

**Frames:**
Frames is designed from the ground up to be type-safe. All operations are checked at compile-time against the schema, which prevents a wide class of bugs.

**Sara:**
Sara's goal is also to provide compile-time type safety, but the implementation is currently inconsistent. Some functions, like `selectColumns` and `dropColumns`, are type-safe, while others fall back to runtime checks. The ongoing refactoring to use `[(Symbol, Type)]` is a step in the right direction, but it needs to be completed and applied consistently across the entire library.

**Recommendation:**
The highest priority for Sara should be to complete the type-safety refactoring. Every function that operates on a `DataFrame` should have its preconditions and postconditions checked at compile-time. This includes aggregations, filtering, joins, and all other data manipulation operations.

## 3. Error Messages

**Frames (and type-level Haskell in general):**
One of the challenges of working with type-level programming in Haskell is that compiler errors can be very difficult to understand.

**Sara:**
As Sara leans more heavily into type-level programming, it will inherit this problem.

**Recommendation:**
Sara could differentiate itself by focusing on providing clear and helpful error messages. This can be achieved through the use of custom type errors (`TypeError` from `GHC.TypeLits`). For example, if a user tries to select a column that doesn't exist, the compiler could produce a user-friendly error message like "Column 'column_name' not found in the DataFrame" instead of a complex type mismatch error.

## 4. Documentation and Tutorials

**Frames:**
The Frames library has a comprehensive tutorial that walks users through its features with clear examples.

**Sara:**
Sara's documentation is currently more limited.

**Recommendation:**
To encourage adoption and make the library easier to learn, Sara needs more extensive documentation. This should include:
- A "getting started" guide.
- A detailed tutorial that covers all of the library's features.
- Clear API documentation for every function, including the type-level constraints.
- Examples of how to use the library to solve common data analysis problems.

## 5. Feature Completeness

**Frames:**
Frames appears to have a more mature and complete feature set, including more advanced operations for data manipulation.

**Sara:**
Sara's feature set is still developing.

**Recommendation:**
Once the type-safety refactoring is complete, Sara should focus on expanding its feature set to be competitive with other data frame libraries. This could include:
- More sophisticated aggregation functions.
- A wider range of join types.
- Time series analysis functions.
- More options for handling missing data.
