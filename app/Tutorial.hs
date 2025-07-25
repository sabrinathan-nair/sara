{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

-- | This module provides a tutorial on how to use the Sara library.
-- It demonstrates how to infer schemas from CSV files, load data into
-- `DataFrame`s, and perform various operations like grouping, aggregation,
-- and mutation.
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
-- This function is not currently used in the main application.
tutorial :: IO ()
tutorial = do
    let employeesStream = readCsvStreaming (Proxy @EmployeesRecord) "employees.csv"
    let dfStream = S.map id employeesStream

    let dfWithBoolColStream = S.map (mutate (Proxy :: Proxy "IsSalaryHigh") (col (Proxy @"EmployeesSalary") >. lit 70000)) dfStream

    let filteredStream = filterByBoolColumn (Proxy :: Proxy "IsSalaryHigh") dfWithBoolColStream
    S.mapM_ print filteredStream

main :: IO ()
main = tutorial