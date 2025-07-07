{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE KindSignatures #-}

module Main where

import Sara.DataFrame.IO (readCSV)
import Sara.DataFrame.Transform
import Sara.DataFrame.Join
import Sara.DataFrame.Aggregate
import Sara.DataFrame.Wrangling (filterRows, filterByBoolColumn)


import qualified Data.Map as Map
import Sara.DataFrame.Types
import Sara.DataFrame.TimeSeries as TimeSeries
import Sara.DataFrame.Missing
import Sara.DataFrame.Statistics as Stats -- Qualify Statistics to avoid ambiguity with TimeSeries.rollingApply
import Sara.DataFrame.Strings as Strings (lower, upper, strip, contains, replace)
import Sara.DataFrame.Expression (Expr, col, lit, (/.!))
import Sara.DataFrame.Predicate ((>.>))

import Data.Time (UTCTime(..), fromGregorian)
import qualified Data.Vector as V
import Data.Proxy (Proxy(..))


-- Helper function to create UTCTime values
createUTCTime :: Integer -> Int -> Int -> UTCTime
createUTCTime y m d = UTCTime (fromGregorian y m d) 0

main :: IO ()
main = do
    putStrLn "Starting the Sara Tutorial..."

    -- 1. Reading Data
    putStrLn "\n--- Reading employees.csv ---"
    employeesDf <- readCSV @'["EmployeeID", "Name", "DepartmentID", "Salary", "StartDate"] "employees.csv"
    print employeesDf

    -- 2. Selecting Columns
    putStrLn "\n--- Selecting Name and Salary columns ---"
    let selectedDf = selectColumns @'["Name", "Salary"] employeesDf
    print selectedDf

    -- 3. Filtering Rows
    putStrLn "\n--- Filtering for employees with Salary > 75000 ---"
    let filteredDf = filterRows (Proxy @"Salary" >.> IntValue 75000) employeesDf
    print filteredDf

    -- 4. Mutating
    putStrLn "\n--- Adding a SalaryInThousands column ---"
    let mutatedDf = mutate (Proxy @"SalaryInThousands") ((col (Proxy @"Salary") :: Expr '["EmployeeID", "Name", "DepartmentID", "Salary", "StartDate"] Double) /.! lit 1000.0) employeesDf
    print mutatedDf

    -- 5. Joining DataFrames
    putStrLn "\n--- Joining employees and departments data ---"
    departmentsDf <- readCSV @'["DepartmentID", "DepartmentName"] "departments.csv"
    let joinedDf = joinDF @'["DepartmentID"] employeesDf departmentsDf LeftJoin
    print joinedDf

    -- 6. Aggregation (Sum of Salaries by Department)
    putStrLn "\n--- Sum of Salaries by Department ---"
    let groupedDf :: GroupedDataFrame '["DepartmentID"] '["EmployeeID", "Name", "DepartmentID", "Salary", "StartDate"]
        groupedDf = groupBy @'["DepartmentID"] employeesDf
    let aggregatedDf :: DataFrame '["DepartmentID", "Salary_sum"]
        aggregatedDf = sumAgg @"Salary" @'["DepartmentID"] groupedDf
    print aggregatedDf

    let sumAggV :: V.Vector DFValue -> DFValue
        sumAggV vec = DoubleValue $ V.sum $ V.map (\val -> case val of IntValue i -> fromIntegral i; DoubleValue d -> d; _ -> 0.0) vec

    -- 7. Time Series Functionality
    putStrLn "\n--- Time Series Analysis ---"
    let timeSeriesData = [
            Map.fromList [("Date", TimestampValue (createUTCTime 2023 1 1)), ("Value", IntValue 10)],
            Map.fromList [("Date", TimestampValue (createUTCTime 2023 1 2)), ("Value", IntValue 12)],
            Map.fromList [("Date", TimestampValue (createUTCTime 2023 1 3)), ("Value", IntValue 15)],
            Map.fromList [("Date", TimestampValue (createUTCTime 2023 1 4)), ("Value", IntValue 13)],
            Map.fromList [("Date", TimestampValue (createUTCTime 2023 1 5)), ("Value", IntValue 18)]
            ]
    let timeSeriesDf = fromRows @'["Date", "Value"] timeSeriesData
    putStrLn "Original Time Series Data:"
    print timeSeriesDf

    -- Resampling
    putStrLn "\n--- Resampling Daily Data to Monthly (Sum) ---"
    let resampledDf = resample (Proxy @"Date") Monthly sumAggV timeSeriesDf
    print resampledDf

    -- Rolling Average
    putStrLn "\n--- Rolling Average (Window 2) on Value ---"
    let rollingDf = TimeSeries.rollingApply (Proxy @"Value") 2 (Stats.meanV :: V.Vector DFValue -> DFValue) timeSeriesDf
    print rollingDf

    -- Shifting
    putStrLn "\n--- Shifting Value column by 1 period ---"
    let shiftedDf = TimeSeries.shift (Proxy @"Value") 1 timeSeriesDf
    print shiftedDf

    -- Percentage Change
    putStrLn "\n--- Percentage Change on Value column ---"
    let pctChangeDf = TimeSeries.pctChange (Proxy @"Value") timeSeriesDf
    print pctChangeDf

    -- 8. Missing Data Handling
    putStrLn "\n--- Missing Data Handling ---"
    let naData = [
            Map.fromList [("ColA", IntValue 1), ("ColB", NA), ("ColC", DoubleValue 1.1)],
            Map.fromList [("ColA", NA), ("ColB", IntValue 2), ("ColC", NA)],
            Map.fromList [("ColA", IntValue 3), ("ColB", NA), ("ColC", DoubleValue 3.3)]
            ]
    let naDf = fromRows @'["ColA", "ColB", "ColC"] naData
    putStrLn "Original DataFrame with NAs:"
    print naDf

    -- Fill NA with a specific value
    putStrLn "\n--- Fill NA with 0 ---"
    let filledDf = fillna naDf Nothing (IntValue 0)
    print filledDf

    -- Forward Fill
    putStrLn "\n--- Forward Fill ---"
    let ffilledDf = ffill naDf
    print ffilledDf

    -- Backward Fill
    putStrLn "\n--- Backward Fill ---"
    let bfilledDf = bfill naDf
    print bfilledDf

    -- Drop rows with any NA
    putStrLn "\n--- Drop Rows with Any NA ---"
    let droppedRowsDf = dropna naDf DropRows Nothing
    print droppedRowsDf

    -- Drop columns with any NA
    putStrLn "\n--- Drop Columns with Any NA ---"
    let droppedColsDf = dropna naDf DropColumns Nothing
    print droppedColsDf

    -- 8.1. isna and notna
    putStrLn "\n--- isna and notna ---"
    let isnaDf = isna naDf
    putStrLn "isna DataFrame:"
    print isnaDf

    let notnaDf = notna naDf
    putStrLn "notna DataFrame:"
    print notnaDf

    -- 9. Statistical Functions
    putStrLn "\n--- Statistical Functions ---"
    let statsData = [
            Map.fromList [("Value", IntValue 10)],
            Map.fromList [("Value", IntValue 20)],
            Map.fromList [("Value", IntValue 15)],
            Map.fromList [("Value", NA)],
            Map.fromList [("Value", IntValue 30)]
            ]
    let statsDf = fromRows @'["Value"] statsData
    putStrLn "Original Data for Statistics:"
    print statsDf

    putStrLn "\n--- Sum of Value Column ---"
    let (DataFrame statsMap) = statsDf
    let sumCol = Map.lookup "Value" statsMap
    case sumCol of
        Just vec -> print $ sumV vec
        Nothing -> putStrLn "Value column not found"

    -- 10. String Functions
    putStrLn "\n--- String Functions ---"
    let stringData = [
            Map.fromList [("TextCol", TextValue "  Hello World  "), ("NumCol", IntValue 1)],
            Map.fromList [("TextCol", TextValue "haskell"), ("NumCol", IntValue 2)],
            Map.fromList [("TextCol", NA), ("NumCol", IntValue 3)]
            ]
    let stringDf = fromRows @'["TextCol", "NumCol"] stringData
    putStrLn "Original Data for String Functions:"
    print stringDf

    putStrLn "\n--- Lowercase TextCol ---"
    let lowerDf = Strings.lower (Proxy @"TextCol") stringDf
    print lowerDf

    putStrLn "\n--- Uppercase TextCol ---"
    let upperDf = Strings.upper (Proxy @"TextCol") stringDf
    print upperDf

    putStrLn "\n--- Strip TextCol ---"
    let stripDf = Strings.strip (Proxy @"TextCol") stringDf
    print stripDf

    putStrLn "\n--- Contains 'World' in TextCol ---"
    let containsDf = Strings.contains (Proxy @"TextCol") "World" stringDf
    print containsDf

    putStrLn "\n--- Replace 'World' with 'Haskell' in TextCol ---"
    let replaceDf = Strings.replace (Proxy @"TextCol") "World" "Haskell" stringDf
    print replaceDf

    -- 11. Boolean Indexing
    putStrLn "\n--- Boolean Indexing ---"
    let boolData = [
            Map.fromList [("Name", TextValue "Alice"), ("IsStudent", BoolValue True)],
            Map.fromList [("Name", TextValue "Bob"), ("IsStudent", BoolValue False)],
            Map.fromList [("Name", TextValue "Charlie"), ("IsStudent", BoolValue True)],
            Map.fromList [("Name", TextValue "David"), ("IsStudent", BoolValue False)]
            ]
    let boolDf = fromRows @'["Name", "IsStudent"] boolData
    putStrLn "Original Data for Boolean Indexing:"
    print boolDf

    putStrLn "\n--- Filter by IsStudent == True ---"
    let filteredBoolDf = filterByBoolColumn (Proxy @"IsStudent") boolDf
    print filteredBoolDf

    -- 12. Column-wise Apply
    putStrLn "\n--- Column-wise Apply ---"
    let applyData = [
            Map.fromList [("ColA", IntValue 1), ("ColB", DoubleValue 10.0)],
            Map.fromList [("ColA", IntValue 2), ("ColB", DoubleValue 20.0)],
            Map.fromList [("ColA", NA), ("ColB", NA)]
            ]
    let applyDf = fromRows @'["ColA", "ColB"] applyData
    putStrLn "Original Data for Column-wise Apply:"
    print applyDf

    putStrLn "\n--- Apply function (multiply by 2) to ColB ---"
    let appliedDf = applyColumn (Proxy @"ColB") (\d -> d * 2 :: Double) applyDf
    print appliedDf

    putStrLn "\n--- Add a new column ColC as ColA + ColB ---"
    let addColDf = addColumn @"ColC" (\row ->
            let valA = Map.lookup "ColA" row
                valB = Map.lookup "ColB" row
            in case (valA, valB) of
                (Just (IntValue a), Just (DoubleValue b)) -> DoubleValue (fromIntegral a + b)
                _ -> NA
            ) applyDf
    print addColDf

    putStrLn "Tutorial finished."
