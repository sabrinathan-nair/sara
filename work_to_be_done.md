### Prioritized Lacking Features (Easiest to Most Difficult, High Utility)

**1. Boolean Indexing (Filtering by a Boolean Column)**
*   **Pandas Utility**: Extremely high. This is a fundamental way to select subsets of data (e.g., `df[df['age'] > 30]`).
*   **Ease of Fulfillment in Sara**: Relatively easy. `sara` already has `filterRows` which takes a predicate. We can create a new function that takes a column name (expected to contain `BoolValue`s) and uses that to construct the predicate for `filterRows`.
*   **Implementable in Sara?**: Yes.

**2. `isna` / `notna` (Boolean Masking for Missing Data)**
*   **Pandas Utility**: Very high. Essential for identifying and handling missing values (e.g., `df.isna()`, `df['col'].notna()`).
*   **Ease of Fulfillment in Sara**: Easy. This involves iterating through a column (or all columns) and returning a new column (or DataFrame) of `BoolValue`s based on whether each `DFValue` is `NA`.
*   **Implementable in Sara?**: Yes.

**3. Column-wise `apply` (General Function Application to Columns)**
*   **Pandas Utility**: High. `df['col'].apply(my_func)` is very powerful for applying arbitrary Python functions to elements of a Series/column.
*   **Ease of Fulfillment in Sara**: Moderate. This would involve a function that takes a DataFrame, a column name, and a `(DFValue -> DFValue)` function, then applies that function element-wise to the specified column. This generalizes many specific transformations.
*   **Implementable in Sara?**: Yes.

**4. More Descriptive Statistics (Median, Mode, Variance, Skew, Kurtosis)**
*   **Pandas Utility**: High. These are standard statistical measures frequently used in data exploration.
*   **Ease of Fulfillment in Sara**: Moderate. `sara` already has `sumV`, `meanV`, `stdV`, `minV`, `maxV`, `countV`. Implementing median and mode requires sorting or frequency counting, which is slightly more complex than simple arithmetic but well within reach using Haskell's standard libraries. Variance, skew, and kurtosis build on existing mean and standard deviation calculations.
*   **Implementable in Sara?**: Yes.

**5. DataFrame-level JSON I/O (Reading/Writing Entire DataFrames as JSON)**
*   **Pandas Utility**: High. JSON is a very common data exchange format.
*   **Ease of Fulfillment in Sara**: Moderate. `sara` already uses `Data.Aeson` for `DFValue`s. Extending this to `DataFrame` would involve defining `ToJSON` and `FromJSON` instances for the `DataFrame` type, likely representing it as a list of JSON objects (rows) or a map of arrays (columns).
*   **Implementable in Sara?**: Yes.

**6. Regex-based String Methods (e.g., `str.contains` with regex, `str.extract`)**
*   **Pandas Utility**: High. Regular expressions are incredibly powerful for complex string pattern matching and extraction.
*   **Ease of Fulfillment in Sara**: Moderate to Difficult. While Haskell has good regex libraries (e.g., `Text.Regex.TDFA`), integrating them to work efficiently across `Vector DFValue`s and handling various regex features (like capturing groups for `extract`) adds complexity.
*   **Implementable in Sara?**: Yes.

**7. `Series` Object**
*   **Pandas Utility**: High. `Series` is the fundamental 1D data structure in `pandas`.
*   **Ease of Fulfillment in Sara**: Moderate to Difficult. While it seems simple (a column with a name), properly integrating it into the `DataFrame` ecosystem (e.g., how `DataFrame` operations return `Series`, how `Series` can be converted to `DataFrame`s, and ensuring consistent indexing) adds significant architectural overhead.
*   **Implementable in Sara?**: Yes.

**8. `split` String Method (that changes column structure)**
*   **Pandas Utility**: High. Used for parsing delimited strings into multiple parts.
*   **Ease of Fulfillment in Sara**: Difficult. The challenge here is that `split` often returns a list of strings for each element, which doesn't directly map to a single `DFValue` type. This would either require a new `DFValue` constructor for lists (complicating other operations) or a strategy to expand the results into multiple new columns, which is a more complex transformation.
*   **Implementable in Sara?**: Yes, but requires careful design decisions about data representation.