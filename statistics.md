# Statistical Functions: Pandas vs. Sara (Missing in Sara)

This document outlines statistical functions commonly available in the Pandas library for Python that are currently *not* explicitly implemented or directly available in the Sara Haskell DataFrame library. This list serves as a roadmap for future development to enhance Sara's statistical capabilities.

| Pandas Function/Feature | Description | Notes for Sara Implementation |
| :---------------------- | :---------- | :---------------------------- |

| `corr()` (Correlation)  | Computes pairwise correlation of columns, excluding NA/null values. Essential for understanding relationships between variables. | Needs functions to calculate Pearson, Spearman, or Kendall correlation coefficients between two numeric columns. |
| `cov()` (Covariance)    | Computes pairwise covariance of columns, excluding NA/null values. Used to determine how two variables change together. | Requires implementation for calculating covariance between two numeric columns. |
| `describe()` (Summary Statistics) | Generates descriptive statistics that summarize the central tendency, dispersion, and shape of a dataset's distribution, excluding `NaN` values. Provides count, mean, std, min, 25%, 50%, 75%, max. | A convenience function that combines several existing (or planned) basic statistics into a single, easy-to-read output. This would involve orchestrating calls to `countV`, `meanV`, `stdV`, `minV`, `maxV`, and the new `quantile` function. |

| `expanding()` (Expanding Window) | Provides expanding transformations (e.g., cumulative sum, cumulative mean). The window grows with the data. | Requires implementation of functions that apply an aggregation over an expanding window, rather than a fixed-size rolling window. |
| `ewm()` (Exponentially Weighted Moving) | Provides exponentially weighted (EW) functions. Useful for time series analysis where more recent observations are given more weight. | Involves implementing EWMA, EWMS, etc., which require specific weighting schemes. |

## Implementation Notes for Sara:

*   **Type Safety:** Any new statistical functions should adhere to Sara's core philosophy of compile-time type safety. This means ensuring that operations are only applied to compatible data types and that schema changes (if any) are reflected at the type level.
*   **Streaming Compatibility:** Given Sara's focus on streaming, new functions should ideally be designed to work efficiently with data streams, allowing for processing of datasets larger than memory.
*   **Performance:** Implementations should be optimized for performance, leveraging Haskell's capabilities for efficient computation.
*   **Error Handling:** Robust error handling should be in place for cases like empty inputs, non-numeric data where numeric is expected, or other invalid operations.
