# Statistical Functions: Pandas vs. Sara (Missing in Sara)

This document outlines statistical functions commonly available in the Pandas library for Python that are currently *not* explicitly implemented or directly available in the Sara Haskell DataFrame library. This list serves as a roadmap for future development to enhance Sara's statistical capabilities.

| Pandas Function/Feature | Description | Notes for Sara Implementation |
| :---------------------- | :---------- | :---------------------------- |





**Note:** `quantile()` and `percentile()` functions have been implemented in Sara.




## Implementation Notes for Sara:

*   **Type Safety:** Any new statistical functions should adhere to Sara's core philosophy of compile-time type safety. This means ensuring that operations are only applied to compatible data types and that schema changes (if any) are reflected at the type level.
*   **Streaming Compatibility:** Given Sara's focus on streaming, new functions should ideally be designed to work efficiently with data streams, allowing for processing of datasets larger than memory.
*   **Performance:** Implementations should be optimized for performance, leveraging Haskell's capabilities for efficient computation.
*   **Error Handling:** Robust error handling should be in place for cases like empty inputs, non-numeric data where numeric is expected, or other invalid operations.
