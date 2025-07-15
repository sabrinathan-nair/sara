{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeApplications #-}

-- | This module provides a tutorial on how to use the Sara library.
-- It demonstrates how to infer schemas from CSV files, load data into
-- `DataFrame`s, and perform various operations like grouping, aggregation,
-- and mutation.
module Main where

import Sara.DataFrame.Static (inferCsvSchema, readCsv)
import Sara.DataFrame.Wrangling
import Sara.DataFrame.Transform
import Sara.DataFrame.Aggregate
import Sara.DataFrame.Expression (col, lit, (>.), (*.*))
import Data.Proxy
import Sara.DataFrame.Internal (toDataFrame)
import qualified Data.Vector as V

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
    Right records <- readCsv "employees.csv" :: IO (Either String (V.Vector EmployeesRecord))
    let df = toDataFrame records

    let groupedDf = groupBy @'[] df
    let _ = sumAgg @"EmployeesSalary" groupedDf

    let _ = mutate (Proxy :: Proxy "EmployeesSalaryX") (col (Proxy @"EmployeesSalary") *.* lit 2) df

    let dfWithBoolCol = mutate (Proxy :: Proxy "IsSalaryHigh") (col (Proxy @"EmployeesSalary") >. lit 70000) df
    let _ = filterByBoolColumn (Proxy :: Proxy "IsSalaryHigh") dfWithBoolCol
    return ()

-- | The main entry point for the application.
-- This is currently a no-op.
main :: IO ()
main = return ()