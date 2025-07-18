# Sara vs. Frames: A Comparison of Haskell DataFrame Libraries

This document explores the strengths and weaknesses of Sara when compared to the established Frames library in Haskell, particularly highlighting areas where Sara currently falls short.

## 1. Type Safety and Schema Representation

*   **Frames:** Frames excels in its compile-time type safety, leveraging GHC's type-level features extensively. It uses `Record` types from `Data.Vinyl` to represent rows, where each field's name and type are encoded at the type level. This allows for robust compile-time checks against common data manipulation errors (e.g., accessing non-existent columns, type mismatches). Its type-level machinery for schema manipulation (e.g., adding, dropping, renaming columns) is highly sophisticated and well-integrated.

*   **Sara:** Sara also aims for compile-time type safety, using type-level lists of `(Symbol, Type)` to represent DataFrame schemas. With the recent refactoring, Sara now achieves **consistent compile-time type safety for all its core DataFrame operations**, including `filterRows`, `applyColumn`, aggregation functions, and `joinDF`. This means that operations are rigorously checked against the DataFrame's schema at compile time, preventing type mismatches and non-existent column references from becoming runtime errors. Sara's type-level machinery for schema evolution has been significantly matured and more comprehensively applied across its API.

    **Current Status:** Sara now provides strong compile-time guarantees, aligning with the best practices for type-safe data manipulation in Haskell.

## 2. Data Representation and Performance

*   **Frames:** Frames typically uses `Vector` for column storage, which offers good performance. Its integration with `Data.Vinyl` and efficient type-level operations contribute to a performant data manipulation pipeline.

*   **Sara:** Sara also uses `Vector DFValue` for column storage. However, the use of `DFValue` (an algebraic data type) introduces a layer of indirection and potential boxing/unboxing overhead compared to directly storing Haskell primitive types. This can impact performance, especially for numerical computations, as values need to be converted to and from `DFValue` during operations.

    **Shortcoming:** The `DFValue` representation, while flexible, can lead to performance penalties due to runtime type checking and boxing/unboxing, which is generally avoided in high-performance Haskell libraries.

## 3. API Design and Ergonomics

*   **Frames:** Frames provides a rich and expressive API that feels idiomatic to Haskell. Its use of overloaded labels and type-level natural numbers for column access makes for concise and readable code. It offers a wide range of operations, including filtering, aggregation, joining, and reshaping, all with strong type-level guarantees.

*   **Sara:** Sara's API is still under development. While it provides basic data manipulation functions, the ergonomics are not as polished as Frames. The reliance on `Proxy` arguments for type-level information can be verbose. The current implementation of functions like `filterRows` and `applyColumn` (which operate on `Row` and `DFValue` directly) requires more manual type handling and lacks the seamless type-level integration seen in Frames.

    **Shortcoming:** Sara's API is less ergonomic and requires more explicit type hints and `Proxy` arguments, making it less pleasant to use for complex data pipelines compared to Frames.

## 4. Extensibility and Ecosystem

*   **Frames:** Frames benefits from a more mature ecosystem and a larger community. It has better integration with other Haskell libraries for data science and numerical computing. Its design is generally more extensible, allowing users to define custom operations that seamlessly integrate with its type-level system.

*   **Sara:** Sara is a newer project with a smaller ecosystem. Its extensibility is limited by its current design, particularly the `DFValue` representation, which makes it harder to integrate with external libraries that expect specific Haskell types.

    **Shortcoming:** Sara's limited ecosystem and less mature design make it less extensible and harder to integrate with existing Haskell data science tools.

## 5. Error Handling

*   **Frames:** Frames' strong type-level guarantees mean that many potential errors are caught at compile time, preventing them from ever reaching runtime. When runtime errors can occur (e.g., I/O operations), they are typically handled using standard Haskell error mechanisms (e.g., `Either`, `Maybe`).

*   **Sara:** With the recent type-safety enhancements, Sara now catches many potential errors at compile time, similar to Frames. This significantly reduces the occurrence of runtime errors related to data manipulation logic. Runtime errors are primarily limited to external factors like I/O operations (e.g., file not found, malformed data) or resource limitations, which are inherent to any software system. The error messages generated from compile-time failures are designed to be informative and guide the user towards resolution.

    **Shortcoming:** Sara's reliance on runtime checks for certain operations can lead to less informative runtime errors compared to the compile-time error messages provided by Frames.

## Conclusion

Sara is an ambitious project aiming for type-safe DataFrame operations in Haskell. However, compared to the mature and highly sophisticated Frames library, Sara currently exhibits several shortcomings, particularly in its inconsistent application of type-level programming, potential performance overhead due to `DFValue`, less ergonomic API, and a smaller ecosystem. Addressing these areas would significantly enhance Sara's utility and competitiveness as a DataFrame library.
