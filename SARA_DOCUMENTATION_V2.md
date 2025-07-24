

# Sara Library Documentation

This document provides a detailed walkthrough of the Sara library, a Haskell framework for type-safe, streaming data manipulation. We will explore the core modules, their functionalities, and the underlying logic that enables high-performance, compile-time guaranteed data processing.

## `app/Tutorial.hs`

This module is a hands-on guide to using Sara. It demonstrates a typical workflow: loading data, adding a new column, filtering based on that column, and printing the results.

**Functionality:**
- **Schema Inference:** Uses Template Haskell to inspect CSV files at compile-time and generate the necessary Haskell types.
- **Streaming Data Processing:** Shows how to process files in chunks (streams) to handle datasets that may not fit into memory.
- **Data Transformation:** Illustrates how to `mutate` a DataFrame by adding new columns and `filter` it based on data conditions.

**Code Walkthrough:**
```haskell
-- These are special instructions for the compiler, called language extensions.
{-# LANGUAGE TemplateHaskell #-} -- Allows code to be run at compile-time (for schema inference)
{-# LANGUAGE DataKinds #-}       -- Allows types to have string labels (for column names)
{-# LANGUAGE TypeApplications #-}  -- A syntax for specifying types explicitly (e.g., Proxy @"ColumnName")

module Main where

import Sara.DataFrame.Static (inferCsvSchema)
import Sara.DataFrame.IO (readCsvStreaming)
import qualified Streaming.Prelude as S
import Sara.DataFrame.Wrangling (filterByBoolColumn)
import Sara.DataFrame.Transform (mutate)
import Sara.DataFrame.Expression (col, lit, (>.))
import Data.Proxy (Proxy(..)) -- A helper for passing types as values

-- | This is the magic of Template Haskell.
-- At compile time, this code opens 'employees.csv', reads its header,
-- and generates two things:
-- 1. A type synonym `Employees` representing the schema (e.g., '["ID" ::: Int, "Name" ::: Text, ...])
-- 2. A record type `EmployeesRecord` for parsing rows from the CSV.
$(inferCsvSchema "Employees" "employees.csv")
$(inferCsvSchema "Departments" "departments.csv")

-- | A tutorial demonstrating the use of the Sara library.
tutorial :: IO ()
tutorial = do
    putStrLn "\n--- Sara Tutorial ---"

    -- 1. Read the CSV file into a stream.
    -- Instead of loading the whole file, `readCsvStreaming` creates a "lazy" stream
    -- that reads the file in chunks. This is memory-efficient.
    putStrLn "1. Reading employees.csv into a DataFrame stream..."
    let employeesStream = readCsvStreaming (Proxy @EmployeesRecord) "employees.csv"

    -- 2. Add a new column.
    -- `S.map` applies the `mutate` function to each DataFrame chunk in the stream.
    -- `mutate` adds a new column named "IsSalaryHigh".
    -- The value for each row is calculated by the expression: is the employee's salary > 70000?
    putStrLn "2. Mutating DataFrame: Adding 'IsSalaryHigh' column..."
    let dfWithBoolColStream = S.map (mutate (Proxy :: Proxy "IsSalaryHigh") (col (Proxy @"EmployeesSalary") >. lit (70000 :: Int))) employeesStream

    -- 3. Filter the stream.
    -- `filterByBoolColumn` inspects each chunk and keeps only the rows where "IsSalaryHigh" is True.
    -- After filtering, it intelligently removes the "IsSalaryHigh" column as it's no longer needed.
    putStrLn "3. Filtering DataFrame: Keeping only employees with high salary..."
    let filteredStream = filterByBoolColumn (Proxy :: Proxy "IsSalaryHigh") dfWithBoolColStream

    -- 4. Print the results.
    -- The stream is only executed now. `S.mapM_ print` pulls each filtered chunk
    -- through the pipeline and prints it to the console.
    putStrLn "4. Printing filtered employees:"
    S.mapM_ print filteredStream

    putStrLn "\n--- Tutorial End ---"

main :: IO ()
main = tutorial -- Run the tutorial
```

## `src/Sara/DataFrame/IO.hs`

This module handles the crucial task of getting data from the outside world into Sara's type-safe `DataFrame` structure. It's built for performance and memory efficiency by using streams.

**Key Function: `readCsvStreaming`**
This function reads a CSV file into a stream of `DataFrame`s.

**Type Signature Explained:**
```haskell
readCsvStreaming :: forall record schema. (HasSchema record schema, FromNamedRecord record)
                 => Proxy record -> FilePath -> S.Stream (Of (DataFrame schema)) IO ()
```
- `forall record schema`: This function is generic and can work with any `record` type and `schema`.
- `(HasSchema record schema, FromNamedRecord record)`: These are compile-time constraints or "rules".
    - `FromNamedRecord record`: The `record` type must be something that the `cassava` library can create from a CSV row. `inferCsvSchema` generates this for us.
    - `HasSchema record schema`: There must be a mapping between the `record` type and the type-level `schema`. `inferCsvSchema` also creates this link.
- `Proxy record`: Since types don't exist at runtime, we pass a "proxy" value to tell the function which record type to use for parsing.
- `S.Stream (Of (DataFrame schema)) IO ()`: This is the return type. It's a stream (`S.Stream`) of `DataFrame`s, where each `DataFrame` is guaranteed by the compiler to have the correct `schema`. The `IO` indicates that the stream performs real-world actions (Input/Output, i.e., reading a file).

**Logic:**
The function uses the `cassava` library to decode the CSV file. If successful, it doesn't load the entire dataset at once. Instead, it wraps the dataset in a stream and uses `S.chunksOf 1000` to yield `DataFrame`s containing 1000 rows each. This "chunking" is the key to its low memory footprint.

## `src/Sara/DataFrame/Wrangling.hs`

This module provides functions for data manipulation, like filtering rows and selecting columns.

**Key Function: `filterByBoolColumn`**
This function filters a stream of `DataFrame`s, keeping only rows where a specific boolean column is `True`. It then cleverly removes that boolean column.

**Type Signature Explained:**
```haskell
filterByBoolColumn :: forall colName schemaRest schema m.
                      (KnownSymbol colName, HasField colName schema Bool, schema ~ (colName ::: Bool ': schemaRest), Monad m)
                   => Proxy colName
                   -> S.Stream (Of (DataFrame schema)) m r
                   -> S.Stream (Of (DataFrame schemaRest)) m r
```
- `KnownSymbol colName`: The column name (`colName`) must be known at compile-time.
- `HasField colName schema Bool`: The input `schema` must contain the `colName`, and its type must be `Bool`. This is a static check; your code won't compile if you try to filter on a non-existent or non-boolean column.
- `schema ~ (colName ::: Bool ': schemaRest)`: This is the most powerful part. It's a type-level equation. It tells the compiler that the input `schema` is composed of the boolean column (`colName ::: Bool`) prepended to the rest of the columns (`schemaRest`). This allows Haskell to infer that the *output* schema will be just `schemaRest`, effectively removing the boolean column automatically and safely.
- `Monad m`: The function is generic over any monad, meaning it can work with streams in `IO` or other contexts.

**Logic:**
The function body `S.map (selectColumns . filterRows (getCol colNameProxy .==. lit True))` is a compact pipeline that operates on each `DataFrame` in the stream:
1.  `filterRows (getCol colNameProxy .==. lit True)`: First, it filters the `DataFrame`, keeping rows where the specified boolean column is `True`.
2.  `selectColumns`: Then, it selects all columns *except* the boolean one. The compiler knows which columns to keep because of the type-level equation in the function's signature.
3.  `S.map`: This applies the two-step process above to every `DataFrame` chunk in the stream.

## `src/Sara/DataFrame/Transform.hs`

This module is for transforming `DataFrame`s, primarily by adding or updating columns.

**Key Function: `mutate`**
Adds a new column to a `DataFrame`. The values in the new column are generated by a function you provide.

**Type Signature Explained:**
```haskell
mutate :: forall colName schema a. (KnownSymbol colName, CanBeDFValue a)
       => Proxy colName
       -> (Row schema -> a)
       -> DataFrame schema
       -> DataFrame (colName ::: a ': schema)
```
- `KnownSymbol colName`: The new column's name must be a compile-time literal.
- `CanBeDFValue a`: The type `a` of the values in the new column must be a type that Sara can store (like `Int`, `Text`, `Bool`).
- `(Row schema -> a)`: This is the function you provide. It takes a `Row` as input and produces a value of type `a`.
- `DataFrame schema -> DataFrame (colName ::: a ': schema)`: This shows the transformation at the type level. It takes a `DataFrame` with `schema` and returns a new `DataFrame` with the new column `colName ::: a` added to the front of the schema.

**Logic:**
1.  It takes the user-provided function `f`.
2.  It maps `f` over every row in the input `DataFrame` to produce a list of new values.
3.  It converts this list into a `Vector`, which is the internal representation for a column.
4.  It calls `addColumn`, which attaches this new column vector to the `DataFrame`, creating the new, larger `DataFrame`.

## `src/Sara/DataFrame/Expression.hs`

This module provides a simple, type-safe Domain Specific Language (DSL) for creating expressions to transform and filter data.

**Logic:**
The key idea is function composition. Instead of working with values directly, functions like `col` and `lit` create *functions* that are later applied to rows.
- `col (Proxy @"Salary")`: This doesn't return a salary. It returns a function `(Row schema -> Int)` that, when given a row, knows how to extract the `"Salary"` field.
- `lit 70000`: This returns a function `(Row schema -> Int)` that, for any row, always returns `70000`.
- `.>.`: This is an operator that takes two such functions (e.g., the ones from `col` and `lit`) and combines them into a new function that takes a row, applies both functions to it, and then compares their results with `>`.

This approach allows you to build complex, reusable transformation logic that is checked for correctness by the compiler.

## `src/Sara/DataFrame/Static.hs`

This module uses Template Haskell (TH) to perform work at compile-time, forming the foundation of Sara's type safety.

**Key Function: `inferCsvSchema`**
This is a TH function that runs during compilation. It reads a CSV file, analyzes its header, and generates Haskell code that is then spliced directly into your program.

**Generated Code:**
For `$(inferCsvSchema "Employees" "employees.csv")`, it generates:
1.  **Schema Type:** A type-level list of column names and their inferred types.
    ```haskell
    type Employees = '["ID" ::: Int, "Name" ::: Text, "Salary" ::: Int, ...]
    ```
2.  **Record Type:** A standard Haskell record for parsing.
    ```haskell
    data EmployeesRecord = EmployeesRecord { employeesID :: Int, employeesName :: Text, ... }
    ```
3.  **Instance Glue:** An instance of the `HasSchema` typeclass that links `EmployeesRecord` to the `Employees` schema, teaching Sara how they relate.

By doing this at compile-time, any mismatch between your code and the CSV file (e.g., you try to access a column that doesn't exist) will be caught by the compiler before your program can even run.

## `src/Sara/DataFrame/Internal.hs`

This module defines the core data structures and type-level machinery of the library.

**Key Components:**
- **`DataFrame schema`**: The central data structure. It's a collection of named, typed columns. The `schema` parameter is a type-level list that tracks the column names and types, enabling compile-time checks.
- **`Schema`**: A type-level list of `(Symbol, Type)` pairs. For example, `type MySchema = '["Name" ::: Text, "Age" ::: Int]`. This is not a value; it's a "ghost" list that only the compiler sees.
- **`Row`**: A single row of data, represented as a heterogeneous list (`HList`). Unlike a normal list, an `HList` can store values of different types, corresponding to the types in the `schema`.
- **`HasSchema`**: A type class that acts as the "glue" between the type-safe `DataFrame schema` world and the external world of loosely typed data (like CSVs). It connects a parsable record type to its corresponding `DataFrame` schema.
