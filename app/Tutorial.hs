module Main where

import Sara.DataFrame.IO
import Sara.DataFrame.Wrangling (filterRows)
import Sara.DataFrame.Transform (selectColumns, addColumn)
import Sara.DataFrame.Join
import Sara.DataFrame.Aggregate
import qualified Data.Text as T
import qualified Data.Map as Map
import Sara.DataFrame.Types (DFValue(..), JoinType(..))

main :: IO ()
main = do
    putStrLn "Starting the Sara Tutorial..."

    -- 1. Reading Data
    putStrLn "\n--- Reading employees.csv ---"
    employeesDf <- readCSV "employees.csv"
    print employeesDf

    -- 2. Selecting Columns
    putStrLn "\n--- Selecting Name and Salary columns ---"
    let selectedDf = selectColumns [T.pack "Name", T.pack "Salary"] employeesDf
    print selectedDf

    -- 3. Filtering Rows
    putStrLn "\n--- Filtering for employees with Salary > 75000 ---"
    let filteredDf = filterRows (\row -> case Map.lookup (T.pack "Salary") row of
                                           Just (IntValue s) -> s > 75000
                                           _ -> False
                               ) employeesDf
    print filteredDf

    -- 4. Mutating
    putStrLn "\n--- Adding a SalaryInThousands column ---"
    let mutatedDf = addColumn (T.pack "SalaryInThousands") (\row -> 
            case Map.lookup (T.pack "Salary") row of
                Just (IntValue s) -> DoubleValue (fromIntegral s / 1000.0)
                _ -> NA
            ) employeesDf
    print mutatedDf

    -- 5. Joining DataFrames
    putStrLn "\n--- Joining employees and departments data ---"
    departmentsDf <- readCSV "departments.csv"
    let joinedDf = joinDF employeesDf departmentsDf [T.pack "DepartmentID"] LeftJoin
    print joinedDf

    -- 6. Aggregation (Sum of Salaries by Department)
    putStrLn "\n--- Sum of Salaries by Department ---"
    let groupedDf = groupBy [T.pack "DepartmentID"] employeesDf
    let aggregatedDf = sumAgg (T.pack "Salary") groupedDf
    print aggregatedDf

    putStrLn "Tutorial finished."
