module Main where

import Sara.DataFrame.Static (inferCsvSchema, readCsv)
import Sara.DataFrame.Types
import Sara.DataFrame.Wrangling
import Sara.DataFrame.Transform
import Sara.DataFrame.Aggregate
import Sara.DataFrame.Expression (col, lit, (>.), (*.*))
import Data.Proxy

$(inferCsvSchema "Employees" "employees.csv")
$(inferCsvSchema "Departments" "departments.csv")

tutorial :: IO ()
tutorial = do
    Right records <- readCsv "employees.csv" :: IO (Either String (V.Vector EmployeesRecord))
    let df = toDataFrame records

    let groupedDf = groupBy @'[] df
    let totalSalary = sumAgg @"EmployeesSalary" groupedDf

    let df2 = mutate (Proxy :: Proxy "EmployeesSalaryX") (col (Proxy @"EmployeesSalary") *.* lit 2) df

    let dfWithBoolCol = mutate (Proxy :: Proxy "IsSalaryHigh") (col (Proxy @"EmployeesSalary") >. lit 70000) df
    let df3 = filterByBoolColumn (Proxy :: Proxy "IsSalaryHigh") dfWithBoolCol

main :: IO ()
main = return ()