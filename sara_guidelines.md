# Sara Library Development Guidelines

This document outlines the core principles and guiding philosophy for the development of the Sara library. All contributors are expected to adhere to these guidelines to ensure the project remains robust, reliable, and maintainable.

## 1. Purpose of the Sara Library

The primary purpose of the Sara library is to provide a powerful, intuitive, and exceptionally safe environment for data manipulation and analysis in Haskell. Our goal is to leverage the full power of the Haskell type system to create a DataFrame-like library that eliminates entire classes of common runtime errors, making data-driven applications more reliable and easier to maintain.

## 2. The Overarching Directive: Type Safety and Compile-Time Guarantees

This is the central pillar of the Sara project. Every function, feature, and module must be designed to maximize type safety. The compiler should be our first line of defense. All operations that can be validated at compile time **must** be validated at compile time. We prefer to spend more time designing a type-safe interface, even if it is more complex internally, than to allow for the possibility of a runtime error.

## 3. Naming Guidelines

Clarity and consistency are paramount. All features, functions, types, and modules should be named in a way that is clear, descriptive, and predictable. Follow existing conventions within the Haskell ecosystem and the project itself. Avoid abbreviations and overly clever names. The name of a function should clearly communicate its purpose and behavior.

## 4. No Dependency on Unmaintained Libraries

To ensure the long-term health, security, and viability of the project, Sara must not depend on any library that is not being actively maintained. Before adding a new dependency, it must be vetted for its maintenance status, community support, and compatibility with the project's long-term goals. Any existing dependency that becomes unmaintained must be flagged for replacement as a matter of priority.

## 5. Focus on Robustness and Reliability over Speed

While performance is important, it is secondary to correctness and reliability. We will not sacrifice type safety or introduce breaking changes for marginal performance gains. The library's reputation must be built on a foundation of trust. Users should be confident that if their code compiles, it will run correctly and predictably.

## 6. Exhaustive Unit Tests for Every Function and Feature

Every function and feature must be accompanied by a comprehensive suite of unit tests. These tests should cover not only the "happy path" but also all edge cases, error conditions, and potential failure modes. A high level of test coverage is non-negotiable and is essential for maintaining the library's quality.

## 7. Maximize Use of QuickCheck for Granular Testing

Beyond standard unit tests, we will leverage property-based testing with QuickCheck to the greatest extent possible. QuickCheck should be used at a granular level to test the fundamental properties and invariants of our functions and types. This is a critical tool for ensuring a bug-free library experience by testing for a much wider range of inputs than is feasible with manual unit tests.

## 8. Investigate and Use Liquid Haskell

To further enhance the robustness and correctness of the library, we will actively investigate and integrate Liquid Haskell. By using refinement types, we can add another layer of compile-time verification, proving properties about our code that go beyond what is possible with the standard GHC type system alone. This aligns perfectly with our core directive of maximizing compile-time guarantees.
