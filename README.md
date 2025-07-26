# Sara: Your Super Smart Data Helper!

Sara is a powerful and intuitive Haskell library for data manipulation, designed with type safety and performance at its core. It leverages the full power of Haskell's type system to provide compile-time guarantees for data operations, eliminating a wide range of common runtime errors. By using a streaming approach, Sara can handle large datasets with a minimal memory footprint.

## Core Features

- **Compile-Time Type Safety:** Sara's API is designed to catch errors at compile time. Operations like selecting a non-existent column or filtering with an incorrect type will result in a compilation error, not a runtime crash.
- **Streaming:** Process large datasets that don't fit in memory. Sara reads and processes data in chunks, ensuring your application remains responsive and memory-efficient.
- **Schema Inference:** Automatically infer data schemas from CSV files at compile time, reducing boilerplate and ensuring your types always match your data.
- **Expressive DSL:** A clear and concise Domain Specific Language (DSL) for common data manipulation tasks like filtering, mutation, and aggregation.
- **Extensible:** Easily add new functionality and integrate with other Haskell libraries.

## Project Status

Sara is currently in a stable state. The core API is well-defined, and the library is suitable for use in production environments. The project is under active development, with a focus on expanding the library's functionality and improving performance.

### Implemented

- **Type-Safe Schemas:** Core data structures are parameterized by type-level schemas.
- **Streaming IO:** Efficiently read CSV and JSON data in a streaming fashion.
- **Wrangling:** Filter, select, and transform data with compile-time guarantees.
- **Expressions:** A type-safe DSL for building complex data transformations.
- **Static Schema Inference:** Automatically generate schemas from CSV files.

### Roadmap

- **Enhanced Aggregation:** Expand the set of aggregation functions and improve their performance.
- **Advanced Joins:** Implement more complex join operations (e.g., outer joins, cross joins).
- **Time Series Analysis:** Add specialized functions for time series data.
- **Visualization:** Integrate with plotting libraries to provide easy data visualization.

## License

Sara is licensed under the MIT License. See the `LICENSE` file for more details.