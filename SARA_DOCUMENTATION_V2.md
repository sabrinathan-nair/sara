### `app/Tutorial.hs`
This module serves as a comprehensive tutorial, demonstrating various functionalities of the Sara library. It showcases how to infer schemas, load data, and perform common DataFrame operations.

**Functionality:**
- **Schema Inference:** Uses `inferCsvSchema` to automatically generate type-level schemas and corresponding record types for `employees.csv` and `departments.csv`.
- **Streaming Data Loading:** Demonstrates reading CSV files into `DataFrame` streams using `readCsvStreaming`.
- **DataFrame Transformations:**
    - **`mutate`:** Adds a new column (`IsSalaryHigh`) based on an expression (`EmployeesSalary > 70000`).
    - **`filterByBoolColumn`:** Filters the DataFrame stream based on the boolean `IsSalaryHigh` column.
- **Interactive Output:** Prints the filtered DataFrame chunks to the console.

**Example:**
```haskell
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Main where

import Sara.DataFrame.Static (inferCsvSchema)
import Sara.DataFrame.IO (readCsvStreaming)
import qualified Streaming.Prelude as S
import Sara.DataFrame.Wrangling (filterByBoolColumn)
import Sara.DataFrame.Transform (mutate)
import Sara.DataFrame.Expression (col, lit, (>.))
import Data.Proxy
import Sara.DataFrame.Internal (HasSchema, Schema)
import Data.Time.Calendar (Day)
import Data.Text (Text)
import Data.Csv (FromNamedRecord)
import GHC.Generics (Generic)

-- | Infer the schema for the `employees.csv` file and create a type synonym `Employees`.
-- It also creates a record type `EmployeesRecord` for parsing the CSV.
$(inferCsvSchema "Employees" "employees.csv")

-- | Infer the schema for the `departments.csv` file and create a type synonym `Departments`.
-- It also creates a record type `DepartmentsRecord` for parsing the CSV.
$(inferCsvSchema "Departments" "departments.csv")

-- | A tutorial demonstrating the use of the Sara library.
tutorial :: IO ()
tutorial = do
    putStrLn "\n--- Sara Tutorial ---"
    putStrLn "1. Reading employees.csv into a DataFrame stream..."
    let employeesStream = readCsvStreaming (Proxy @EmployeesRecord) "employees.csv"

    putStrLn "2. Mutating DataFrame: Adding 'IsSalaryHigh' column..."
    let dfWithBoolColStream = S.map (mutate (Proxy :: Proxy "IsSalaryHigh") (col (Proxy @"EmployeesSalary") >. lit (70000 :: Int))) employeesStream

    putStrLn "3. Filtering DataFrame: Keeping only employees with high salary..."
    let filteredStream = filterByBoolColumn (Proxy :: Proxy "IsSalaryHigh") dfWithBoolColStream

    putStrLn "4. Printing filtered employees:"
    S.mapM_ print filteredStream

    putStrLn "\n--- Tutorial End ---"

-- | The main entry point for the application.
-- This is currently a no-op, but you can call 'tutorial' here to run it.
main :: IO ()
main = tutorial -- Call the tutorial function
```