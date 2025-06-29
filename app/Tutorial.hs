module Main where

import Sara.DataFrame.IO
import Sara.DataFrame.Wrangling (filterRows, filterByBoolColumn)
import Sara.DataFrame.Transform (selectColumns, addColumn, applyColumn)
import Sara.DataFrame.Join
import Sara.DataFrame.Aggregate
import qualified Data.Text as T
import qualified Data.Map as Map
import Sara.DataFrame.Types (DFValue(..), JoinType(..), Row, DataFrame(..))
import Sara.DataFrame.TimeSeries (resample, shift, pctChange, fromRows, ResampleRule(..))
import Sara.DataFrame.Missing (fillna, ffill, bfill, dropna, DropAxis(..), isna, notna)
import Sara.DataFrame.Statistics (rollingApply, sumV, meanV, stdV, minV, maxV, countV)
import Sara.DataFrame.Strings (lower, upper, strip, contains, replace)
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
    let rollingDf = rollingApply timeSeriesDf 2 "Value" meanV
    print rollingDf

    -- Shifting
    putStrLn "\n--- Shifting Value column by 1 period ---"
    let shiftedDf = shift timeSeriesDf "Value" 1
    print shiftedDf

    -- Percentage Change
    putStrLn "\n--- Percentage Change on Value column ---"
    let pctChangeDf = pctChange timeSeriesDf "Value"
    print pctChangeDf

    -- 8. Missing Data Handling
    putStrLn "\n--- Missing Data Handling ---"
    let naData = [
            Map.fromList [(T.pack "ColA", IntValue 1), (T.pack "ColB", NA), (T.pack "ColC", DoubleValue 1.1)],
            Map.fromList [(T.pack "ColA", NA), (T.pack "ColB", IntValue 2), (T.pack "ColC", NA)],
            Map.fromList [(T.pack "ColA", IntValue 3), (T.pack "ColB", NA), (T.pack "ColC", DoubleValue 3.3)]
            ]
    let naDf = fromRows naData
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
            Map.fromList [(T.pack "Value", IntValue 10)],
            Map.fromList [(T.pack "Value", IntValue 20)],
            Map.fromList [(T.pack "Value", IntValue 15)],
            Map.fromList [(T.pack "Value", NA)],
            Map.fromList [(T.pack "Value", IntValue 30)]
            ]
    let statsDf = fromRows statsData
    putStrLn "Original Data for Statistics:"
    print statsDf

    putStrLn "\n--- Sum of Value Column ---"
    let sumCol = Map.lookup (T.pack "Value") (case statsDf of DataFrame m -> m)
    case sumCol of
        Just vec -> print $ sumV vec
        Nothing -> putStrLn "Value column not found"

    putStrLn "\n--- Mean of Value Column ---"
    case sumCol of
        Just vec -> print $ meanV vec
        Nothing -> putStrLn "Value column not found"

    putStrLn "\n--- Standard Deviation of Value Column ---"
    case sumCol of
        Just vec -> print $ stdV vec
        Nothing -> putStrLn "Value column not found"

    putStrLn "\n--- Minimum of Value Column ---"
    case sumCol of
        Just vec -> print $ minV vec
        Nothing -> putStrLn "Value column not found"

    putStrLn "\n--- Maximum of Value Column ---"
    case sumCol of
        Just vec -> print $ maxV vec
        Nothing -> putStrLn "Value column not found"

    putStrLn "\n--- Count of Non-NA Values in Value Column ---"
    case sumCol of
        Just vec -> print $ countV vec
        Nothing -> putStrLn "Value column not found"

    -- 10. String Functions
    putStrLn "\n--- String Functions ---"
    let stringData = [
            Map.fromList [(T.pack "TextCol", TextValue (T.pack "  Hello World  ")), (T.pack "NumCol", IntValue 1)],
            Map.fromList [(T.pack "TextCol", TextValue (T.pack "haskell")), (T.pack "NumCol", IntValue 2)],
            Map.fromList [(T.pack "TextCol", NA), (T.pack "NumCol", IntValue 3)]
            ]
    let stringDf = fromRows stringData
    putStrLn "Original Data for String Functions:"
    print stringDf

    putStrLn "\n--- Lowercase TextCol ---"
    let lowerDf = lower stringDf (T.pack "TextCol")
    print lowerDf

    putStrLn "\n--- Uppercase TextCol ---"
    let upperDf = upper stringDf (T.pack "TextCol")
    print upperDf

    putStrLn "\n--- Strip TextCol ---"
    let stripDf = strip stringDf (T.pack "TextCol")
    print stripDf

    putStrLn "\n--- Contains 'World' in TextCol ---"
    let containsDf = contains stringDf (T.pack "TextCol") (T.pack "World")
    print containsDf

    putStrLn "\n--- Replace 'World' with 'Haskell' in TextCol ---"
    let replaceDf = replace stringDf (T.pack "TextCol") (T.pack "World") (T.pack "Haskell")
    print replaceDf

    -- 11. Boolean Indexing
    putStrLn "\n--- Boolean Indexing ---"
    let boolData = [
            Map.fromList [(T.pack "Name", TextValue (T.pack "Alice")), (T.pack "IsStudent", BoolValue True)],
            Map.fromList [(T.pack "Name", TextValue (T.pack "Bob")), (T.pack "IsStudent", BoolValue False)],
            Map.fromList [(T.pack "Name", TextValue (T.pack "Charlie")), (T.pack "IsStudent", BoolValue True)],
            Map.fromList [(T.pack "Name", TextValue (T.pack "David")), (T.pack "IsStudent", BoolValue False)]
            ]
    let boolDf = fromRows boolData
    putStrLn "Original Data for Boolean Indexing:"
    print boolDf

    putStrLn "\n--- Filter by IsStudent == True ---"
    let filteredBoolDf = filterByBoolColumn boolDf (T.pack "IsStudent")
    print filteredBoolDf

    -- 12. Column-wise Apply
    putStrLn "\n--- Column-wise Apply ---"
    let applyData = [
            Map.fromList [(T.pack "ColA", IntValue 1), (T.pack "ColB", DoubleValue 10.0)],
            Map.fromList [(T.pack "ColA", IntValue 2), (T.pack "ColB", DoubleValue 20.0)],
            Map.fromList [(T.pack "ColA", NA), (T.pack "ColB", NA)]
            ]
    let applyDf = fromRows applyData
    putStrLn "Original Data for Column-wise Apply:"
    print applyDf

    putStrLn "\n--- Apply function (multiply by 2) to ColB ---"
    let appliedDf = applyColumn (T.pack "ColB") (\val -> case val of DoubleValue d -> DoubleValue (d * 2); IntValue i -> DoubleValue (fromIntegral i * 2); _ -> NA) applyDf
    print appliedDf
