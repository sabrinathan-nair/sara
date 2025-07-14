### 9. Core Data Structures and Type Classes (`Sara.DataFrame.Types`)

This module defines the fundamental data structures and type classes that underpin Sara's type-safe DataFrame operations.

#### `DFValue`

```haskell
data DFValue = IntValue Int
           | DoubleValue Double
           | TextValue T.Text
           | DateValue Day
           | TimestampValue UTCTime
           | BoolValue Bool
           | NA -- ^ Represents a missing value.
           deriving (Show, Eq, Ord, Generic, NFData)
```

A type to represent a single value within a DataFrame cell. It's an algebraic data type that can hold various primitive Haskell types, as well as a special `NA` constructor for missing values.

**Constructors:**
*   `IntValue Int`: An integer value.
*   `DoubleValue Double`: A double-precision floating-point value.
*   `TextValue T.Text`: A text string.
*   `DateValue Day`: A date value.
*   `TimestampValue UTCTime`: A date and time value.
*   `BoolValue Bool`: A boolean value.
*   `NA`: Represents a missing or null value.

**Considerations:**
*   `DFValue` is the runtime representation of data in a DataFrame. Type safety is achieved by ensuring that operations on `DFValue`s are consistent with the type-level schema.

#### `Column`

```haskell
type Column = Vector DFValue
```

A type alias for a column in a DataFrame, represented as a `Vector` of `DFValue`s. `Vector` provides efficient storage and operations for homogeneous collections.

#### `DataFrame`

```haskell
newtype DataFrame (cols :: [(Symbol, Type)]) = DataFrame (Map T.Text Column)
```

The core DataFrame data structure. It's a `newtype` wrapper around a `Map` from column names (`T.Text`) to `Column`s. The `cols` type parameter is a type-level list that encodes the DataFrame's schema (column names and their types) at compile time.

**Parameters:**
*   `cols`: A type-level list of `(Symbol, Type)` tuples representing the DataFrame's schema.

**Considerations:**
*   The `newtype` wrapper provides strong type safety by preventing direct manipulation of the underlying `Map`, ensuring that all operations go through type-checked functions.

#### `Row`

```haskell
type Row = Map T.Text DFValue
```

A type alias for a row in a DataFrame, represented as a `Map` from column names (`T.Text`) to `DFValue`s.

#### `toRows`

```haskell
toRows :: DataFrame cols -> [Row]
```

Converts a `DataFrame` into a list of `Row`s.

**Parameters:**
*   `DataFrame cols`: The input DataFrame.

**Returns:**
*   `[Row]`: A list of `Row`s, where each `Row` is a `Map` from column names to `DFValue`s.

#### `fromRows`

```haskell
fromRows :: KnownColumns cols => [Row] -> DataFrame cols
```

Creates a `DataFrame` from a list of `Row`s. The `KnownColumns` constraint ensures that the schema of the resulting DataFrame is known at compile time.

**Parameters:**
*   `[Row]`: A list of `Row`s.

**Returns:**
*   `DataFrame cols`: A new DataFrame constructed from the input rows.

#### `SortOrder`

```haskell
data SortOrder = Ascending | Descending
    deriving (Show, Eq)
```

Specifies the order in which a column should be sorted.

**Constructors:**
*   `Ascending`: Sort in ascending order.
*   `Descending`: Sort in descending order.

#### `ConcatAxis`

```haskell
data ConcatAxis = ConcatRows | ConcatColumns
    deriving (Show, Eq)
```

Specifies the axis along which DataFrames should be concatenated.

**Constructors:**
*   `ConcatRows`: Concatenate DataFrames row-wise.
*   `ConcatColumns`: Concatenate DataFrames column-wise.

#### `JoinType`

```haskell
data JoinType = InnerJoin | LeftJoin | RightJoin | OuterJoin
    deriving (Show, Eq)
```

Specifies the type of join to perform when merging DataFrames.

**Constructors:**
*   `InnerJoin`: Returns only the rows that have matching keys in both DataFrames.
*   `LeftJoin`: Returns all rows from the left DataFrame, and the matched rows from the right DataFrame.
*   `RightJoin`: Returns all rows from the right DataFrame, and the matched rows from the left DataFrame.
*   `OuterJoin`: Returns all rows when there is a match in one of the DataFrames.

#### `SortCriterion`

```haskell
data SortCriterion (cols :: [(Symbol, Type)]) where
    SortCriterion :: (KnownSymbol col, HasColumn col cols, Ord (TypeOf col cols), CanBeDFValue (TypeOf col cols)) => Proxy col -> SortOrder -> SortCriterion cols
```

A type-safe criterion for sorting a DataFrame. It specifies a column to sort by and the sort order.

**Parameters:**
*   `col`: A type-level `Symbol` representing the column name.
*   `cols`: The schema of the DataFrame being sorted.
*   `Ord (TypeOf col cols)`: A constraint ensuring that the type of the column has an `Ord` instance, meaning it can be ordered.
*   `CanBeDFValue (TypeOf col cols)`: A constraint ensuring that the type of the column can be converted to/from `DFValue`.
*   `Proxy col`: A `Proxy` carrying the type-level column name.
*   `SortOrder`: The desired sort order (`Ascending` or `Descending`).

#### `SortableColumn`

```haskell
type SortableColumn (col :: Symbol) (cols :: [(Symbol, Type)]) = (KnownSymbol col, HasColumn col cols)
```

A type synonym for a column that can be sorted. It ensures the column exists and its name is known at compile time.

#### `KnownColumns`

```haskell
class KnownColumns (cols :: [(Symbol, Type)]) where
    columnNames :: Proxy cols -> [T.Text]
    columnTypes :: Proxy cols -> [TypeRep]
```

A type class to extract runtime information (column names and types) from a type-level list of `(Symbol, Type)` tuples. This bridges the gap between compile-time schema and runtime operations.

#### `CanBeDFValue`

```haskell
class CanBeDFValue a where
    toDFValue :: a -> DFValue
    fromDFValue :: DFValue -> Maybe a
    fromDFValueUnsafe :: DFValue -> a
    default fromDFValueUnsafe :: DFValue -> a
    fromDFValueUnsafe dfValue = case fromDFValue dfValue of
        Just x -> x
        Nothing -> error "fromDFValueUnsafe: Type mismatch or NA value"
```

A type class for values that can be safely converted to and from `DFValue`. This ensures that only supported types can be stored in a DataFrame.

**Methods:**
*   `toDFValue :: a -> DFValue`: Converts a value of type `a` to a `DFValue`.
*   `fromDFValue :: DFValue -> Maybe a`: Safely converts a `DFValue` to a `Maybe a`. Returns `Nothing` if the `DFValue` is `NA` or a type mismatch occurs.
*   `fromDFValueUnsafe :: DFValue -> a`: Unsafely converts a `DFValue` to type `a`. **Use with caution**, as it will throw a runtime error if the conversion fails.

#### `Append`

```haskell
type family Append (xs :: [k]) (ys :: [k]) :: [k]
```

A type family that appends two type-level lists. Used for combining schemas.

#### `Remove`

```haskell
type family Remove (x :: k) (ys :: [k]) :: [k]
```

A type family that removes an element from a type-level list. Used for schema manipulation.

#### `Nub`

```haskell
type family Nub (xs :: [k]) :: [k]
```

A type family that removes duplicates from a type-level list. Used for schema manipulation.

#### `HasColumn`

```haskell
type HasColumn (col :: Symbol) (cols :: [(Symbol, Type)]) = (KnownSymbol col, CheckHasColumn col cols)
```

A constraint synonym that checks if a column exists in a DataFrame's schema at compile time. If the column is not found, it produces a custom `TypeError` message.

#### `HasColumns`

```haskell
type HasColumns (subset :: [Symbol]) (superset :: [(Symbol, Type)]) :: Constraint
```

A constraint that ensures a list of columns (`subset`) exists within another list of columns (`superset`) at compile time.

#### `JoinCols`

```haskell
type family JoinCols (cols1 :: [(Symbol, Type)]) (cols2 :: [(Symbol, Type)]) (joinType :: JoinType) :: [(Symbol, Type)]
```

A type family that computes the schema of a DataFrame resulting from a join operation. It infers the new column names and types based on the input schemas and the `JoinType`.

#### `TypeOf`

```haskell
type family TypeOf (col :: Symbol) (cols :: [(Symbol, Type)]) :: Type
```

A type family that gets the type of a column given its name and the DataFrame's schema. If the column is not found, it produces a custom `TypeError` message.

#### `MapSymbols`

```haskell
type family MapSymbols (xs :: [(Symbol, Type)]) :: [Symbol]
```

A helper type family to extract just the `Symbol`s (column names) from a list of `(Symbol, Type)` tuples.

#### `UpdateColumn`

```haskell
type family UpdateColumn (colName :: Symbol) (newType :: Type) (cols :: [(Symbol, Type)]) :: [(Symbol, Type)]
```

A type family that updates the type of a column in a schema. If the column is not found, it produces a custom `TypeError` message.

#### `TypeLevelRow`

```haskell
newtype TypeLevelRow (cols :: [(Symbol, Type)]) = TypeLevelRow (Map T.Text DFValue)
    deriving (Show, Eq, Ord)
```

A type to represent a single row with its schema tracked at the type level. Used internally for grouping operations.

#### `toTypeLevelRow`

```haskell
toTypeLevelRow :: forall cols. KnownColumns cols => Row -> TypeLevelRow cols
```

Converts a runtime `Row` (Map `Text` `DFValue`) to a type-level `TypeLevelRow`, ensuring schema conformity.

#### `fromTypeLevelRow`

```haskell
fromTypeLevelRow :: TypeLevelRow cols -> Row
```

Converts a type-level `TypeLevelRow` back to a runtime `Row`.

#### `isNA`

```haskell
isNA :: DFValue -> Bool
```

Checks if a `DFValue` is `NA`.

#### `MapTypes`

```haskell
type family MapTypes (xs :: [(Symbol, Type)]) :: [Type]
```

A type family that extracts just the `Type`s from a list of `(Symbol, Type)` tuples.

#### `All`

```haskell
type family All (c :: k -> Constraint) (xs :: [k]) :: Constraint
```

A type family that applies a constraint `c` to all elements `x` in a type-level list `xs`.

#### `ContainsColumn`

```haskell
type family ContainsColumn (s :: Symbol) (cols :: [(Symbol, Type)]) :: Bool
```

A type family that checks if a `Symbol` (column name) is present in a DataFrame's schema at the type level.

#### `ResolveJoinValue`

```haskell
type family ResolveJoinValue (a :: Type) (b :: Type) (joinType :: JoinType) :: Type
```

A type family that determines the resulting type of a single value when joining two columns based on the `JoinType`.

#### `ResolveJoinValueType`

```haskell
type family ResolveJoinValueType (a :: Type) (b :: Type) (joinType :: JoinType) :: Type
```

Similar to `ResolveJoinValue`, but specifically for resolving the type of a column in the joined DataFrame.

#### `resolveJoinValueImpl`

```haskell
resolveJoinValueImpl :: DFValue -> DFValue -> JoinType -> DFValue
```

The runtime implementation of `ResolveJoinValueType`, handling the actual `DFValue`s during a join operation.