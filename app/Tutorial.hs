module Main where

import Sara.DataFrame.IO
import Sara.DataFrame.Wrangling (filterRows)
import Sara.DataFrame.Transform (selectColumns, addColumn)
import Sara.DataFrame.Join
import Sara.DataFrame.Aggregate
import qualified Data.Text as T
import qualified Data.Map as Map
import Sara.DataFrame.Types (DFValue(..), JoinType(..), Row)
import Sara.DataFrame.TimeSeries (resample, rolling, shift, pctChange, fromRows, ResampleRule(..))
import Data.Time.Format (parseTimeM, defaultTimeLocale)
import Data.Time (UTCTime)
import qualified Data.Vector as V

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

    let sumAggV :: V.Vector DFValue -> DFValue
        sumAggV vec = DoubleValue $ V.sum $ V.map (\val -> case val of IntValue i -> fromIntegral i; DoubleValue d -> d; _ -> 0.0) vec

    putStrLn "Tutorial finished."

    -- 7. Time Series Functionality
    putStrLn "\n--- Time Series Analysis ---"
    let timeSeriesData = [
            Map.fromList [(T.pack "Date", DateValue (read "2023-01-01")), (T.pack "Value", IntValue 10)],
            Map.fromList [(T.pack "Date", DateValue (read "2023-01-02")), (T.pack "Value", IntValue 12)],
            Map.fromList [(T.pack "Date", DateValue (read "2023-01-03")), (T.pack "Value", IntValue 15)],
            Map.fromList [(T.pack "Date", DateValue (read "2023-01-04")), (T.pack "Value", IntValue 13)],
            Map.fromList [(T.pack "Date", DateValue (read "2023-01-05")), (T.pack "Value", IntValue 18)]
            ]
    let timeSeriesDf = fromRows timeSeriesData
    putStrLn "Original Time Series Data:"
    print timeSeriesDf

    -- Resampling
    putStrLn "\n--- Resampling Daily Data to Monthly (Sum) ---"
    let resampledDf = resample timeSeriesDf "Date" Monthly (sumAggV)
    print resampledDf

    -- Rolling Average
    putStrLn "\n--- Rolling Average (Window 2) on Value ---"
    let rollingDf = rolling timeSeriesDf 2 "Value"
    print rollingDf

    -- Shifting
    putStrLn "\n--- Shifting Value column by 1 period ---"
    let shiftedDf = shift timeSeriesDf "Value" 1
    print shiftedDf

    -- Percentage Change
    putStrLn "\n--- Percentage Change on Value column ---"
    let pctChangeDf = pctChange timeSeriesDf "Value"
    print pctChangeDf
