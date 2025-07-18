# Sara: A Type-Safe Data Science Library in Haskell

Sara is a data science library written in Haskell, designed with a strong emphasis on compile-time type safety. By leveraging advanced type-level programming techniques, Sara aims to eliminate common runtime errors associated with data manipulation, providing robust and reliable data processing capabilities.

## Project Goals

The primary goal of Sara is to provide a data manipulation framework where the correctness of data operations, especially concerning schema and column types, is verified at compile-time. This approach significantly reduces the likelihood of runtime errors, leading to more stable and predictable data science workflows.

## Key Features

-   **Compile-Time Type Safety:** All core DataFrame operations, including filtering, aggregation, joining, and column transformations, are type-checked at compile time. This ensures that operations are only performed on compatible data types and existing columns.
-   **Type-Level Programming:** Sara extensively uses Haskell's type-level features (e.g., `DataKinds`, `TypeFamilies`, `GHC.TypeLits`) to represent DataFrame schemas and enforce constraints at the type level.
-   **DataFrame Operations:** Provides a rich set of operations for data wrangling, including:
    -   Filtering rows based on type-safe predicates.
    -   Applying functions to columns with type-checked transformations.
    -   Sorting DataFrames by specified criteria.
    -   Mutating DataFrames by adding or modifying columns based on expressions.
    -   Performing type-safe aggregations (sum, mean, count) on grouped data.
    -   Joining DataFrames with compile-time schema resolution.
    -   Dropping and selecting columns.
-   **CSV/JSON Integration:** Includes utilities for reading and writing DataFrames from/to CSV and JSON files, with schema inference capabilities.

## Current Status

As of the latest update, Sara offers **100% compile-time guarantees for its core DataFrame operations**. This means that type mismatches, non-existent column references, and other schema-related errors will be caught during compilation, preventing them from becoming runtime issues.

While the library strives for maximum type safety, it's important to note that certain runtime errors related to external factors (e.g., file not found, malformed external data, resource exhaustion) are inherent to I/O operations and system limitations. However, the internal data manipulation logic is rigorously type-checked.

## Installation and Building

To build the Sara library and run its tests, you will need the [Haskell Tool Stack](https://docs.haskellstack.org/en/stable/README/) or [Cabal](https://www.haskell.org/cabal/).

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/sabrinathan-nair/sara.git
    cd sara
    ```

2.  **Build the project:**
    ```bash
    cabal build
    # or stack build
    ```

3.  **Run the tests:**
    ```bash
    cabal test
    # or stack test
    ```

## Usage Examples

(Coming soon: Detailed code examples demonstrating various DataFrame operations and their type-safe usage.)

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.