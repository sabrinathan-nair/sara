# Urgent Technical Debt and Modernization Plan

## Summary

This document summarizes a critical strategic issue identified for the Sara project. The project's long-term viability is at risk due to its reliance on several unmaintained dependencies. This technical debt currently blocks any upgrade to a modern GHC version (e.g., GHC 9.10.1 or newer) and poses significant future risks.

## The Core Problem

The project cannot be upgraded to a modern version of GHC. The root cause is not a specific GHC incompatibility, but a dependency on three libraries that appear to be abandoned by their maintainers.

The unmaintained libraries are:
1.  `cassava-streams`
2.  `json-stream`
3.  `vector-strategies`

## Risks

Continuing to rely on these libraries introduces several compounding risks:

*   **Obsolescence:** The project is at risk of becoming obsolete as the wider Haskell ecosystem evolves.
*   **Security Vulnerabilities:** Unmaintained libraries do not receive security patches.
*   **Bit Rot:** It will become progressively harder to compile and run the project on modern systems.
*   **Blocked Progress:** We are prevented from leveraging the features, bug fixes, and performance improvements of new GHC versions and updated libraries.
*   **Low Contributor Appeal:** The project will struggle to attract new contributors if it is stuck on an outdated technology stack.

## Proposed Action Plan

To address this technical debt and ensure the project's future, a focused modernization effort is required. The following three-step plan will replace the problematic libraries with modern, actively maintained alternatives:

1.  **Replace `vector-strategies`:** The functionality can be replaced with the `parallel` library, which is already a dependency.
2.  **Replace `cassava-streams`:** The `streaming-cassava` library is the modern standard for this functionality.
3.  **Replace `json-stream`:** A modern streaming JSON library, such as `aeson-stream`, should be used as a replacement.

This work is the highest priority for ensuring the long-term health and success of the Sara project.
