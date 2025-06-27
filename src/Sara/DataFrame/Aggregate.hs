module Sara.DataFrame.Aggregate (
    GroupedDataFrame,
    groupBy,
    sumAgg,
    meanAgg,
    countAgg,
    pivotTable
) where

import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.List (foldl', nub)
import Sara.DataFrame.Types (DataFrame(..), Column, Row, toRows, DFValue(..))
import Sara.DataFrame.Wrangling (filterRows)

-- | A DataFrame grouped by certain columns.
-- The outer Map's keys are the unique combinations of values from the grouping columns (represented as a Row),
-- and the values are DataFrames containing the rows belonging to that group.
type GroupedDataFrame = Map Row DataFrame

-- | Groups a DataFrame by the specified column names.
groupBy :: [T.Text] -> DataFrame -> GroupedDataFrame
groupBy groupCols df =
    let
        rows = toRows df
        -- Function to extract the group key from a row
        getGroupKey :: Row -> Row
        getGroupKey row = Map.filterWithKey (\k _ -> k `elem` groupCols) row

        -- Fold over rows to build the GroupedDataFrame
        initialGroupedMap = Map.empty :: GroupedDataFrame
        groupedMap = foldl' (\accMap row ->
            let
                groupKey = getGroupKey row
                -- Convert the single row back to a DataFrame
                singleRowDf = DataFrame $ Map.map (\val -> V.singleton val) row
            in
                Map.insertWith (\newDf existingDf ->
                    let
                        (DataFrame newMap) = newDf
                        (DataFrame existingMap) = existingDf
                        -- Merge columns of newDf into existingDf
                        mergedMap = Map.unionWith (V.++) existingMap newMap
                    in
                        DataFrame mergedMap
                ) groupKey singleRowDf accMap
            ) initialGroupedMap rows
    in
        groupedMap

-- | Aggregates a GroupedDataFrame by summing a specified column.
sumAgg :: T.Text -> GroupedDataFrame -> DataFrame
sumAgg colName groupedDf =
    let
        aggregatedRows = Map.foldlWithKey (
            \accMap groupKey dfGroup ->
                let (DataFrame df) = dfGroup in
                case Map.lookup colName df of
                    Just col ->
                        let
                            totalSum = V.foldl' (
                                \acc val -> case val of
                                    IntValue i -> acc + fromIntegral i
                                    DoubleValue d -> acc + d
                                    _ -> acc
                                ) 0.0 col
                        in
                            Map.insert groupKey (DoubleValue totalSum) accMap
                    Nothing -> accMap
            ) Map.empty groupedDf
    in
        if Map.null aggregatedRows
            then DataFrame Map.empty
            else
                let
                    -- Convert aggregated Map to DataFrame
                    allGroupColNames = foldl' (\acc keyRow -> Map.keys keyRow ++ acc) [] (Map.keys aggregatedRows)
                    uniqueGroupColNames = nub $ Map.keys $ head $ Map.keys aggregatedRows -- Assuming at least one group

                    -- Create columns for the group keys
                    groupKeyColumns = Map.fromList $ map (\name ->
                        (name, V.fromList $ map (\keyRow -> Map.findWithDefault NA name keyRow) (Map.keys aggregatedRows))
                        ) uniqueGroupColNames

                    -- Create the aggregated value column
                    aggColumn = V.fromList $ Map.elems aggregatedRows

                    -- Combine group key columns and aggregated column into a new DataFrame
                    finalDfMap = Map.insert (colName `T.append` T.pack "_sum") aggColumn groupKeyColumns
                in
                    DataFrame finalDfMap

-- | Aggregates a GroupedDataFrame by calculating the mean of a specified column.
meanAgg :: T.Text -> GroupedDataFrame -> DataFrame
meanAgg colName groupedDf =
    let
        aggregatedRows = Map.foldlWithKey (
            \accMap groupKey dfGroup ->
                let (DataFrame df) = dfGroup in
                case Map.lookup colName df of
                    Just col ->
                        let
                            (totalSum, count) = V.foldl' (
                                \(accSum, accCount) val -> case val of
                                    IntValue i -> (accSum + fromIntegral i, accCount + 1)
                                    DoubleValue d -> (accSum + d, accCount + 1)
                                    _ -> (accSum, accCount)
                                ) (0.0, 0) col
                            meanVal = if count > 0 then DoubleValue (totalSum / fromIntegral count) else NA
                        in
                            Map.insert groupKey meanVal accMap
                    Nothing -> accMap
            ) Map.empty groupedDf
    in
        if Map.null aggregatedRows
            then DataFrame Map.empty
            else
                let
                    -- Convert aggregated Map to DataFrame
                    uniqueGroupColNames = nub $ Map.keys $ head $ Map.keys aggregatedRows
                    groupKeyColumns = Map.fromList $ map (\name ->
                        (name, V.fromList $ map (\keyRow -> Map.findWithDefault NA name keyRow) (Map.keys aggregatedRows))
                        ) uniqueGroupColNames
                    aggColumn = V.fromList $ Map.elems aggregatedRows
                    finalDfMap = Map.insert (colName `T.append` T.pack "_mean") aggColumn groupKeyColumns
                in
                    DataFrame finalDfMap

-- | Aggregates a GroupedDataFrame by counting non-NA values in a specified column.
countAgg :: T.Text -> GroupedDataFrame -> DataFrame
countAgg colName groupedDf =
    let
        aggregatedRows = Map.foldlWithKey (
            \accMap groupKey dfGroup ->
                let (DataFrame df) = dfGroup in
                case Map.lookup colName df of
                    Just col ->
                        let
                            count = V.foldl' (
                                \acc val -> case val of
                                    NA -> acc
                                    _ -> acc + 1
                                ) 0 col
                        in
                            Map.insert groupKey (IntValue count) accMap
                    Nothing -> accMap
            ) Map.empty groupedDf
    in
        if Map.null aggregatedRows
            then DataFrame Map.empty
            else
                let
                    -- Convert aggregated Map to DataFrame
                    uniqueGroupColNames = nub $ Map.keys $ head $ Map.keys aggregatedRows
                    groupKeyColumns = Map.fromList $ map (\name ->
                        (name, V.fromList $ map (\keyRow -> Map.findWithDefault NA name keyRow) (Map.keys aggregatedRows))
                        ) uniqueGroupColNames
                    aggColumn = V.fromList $ Map.elems aggregatedRows
                    finalDfMap = Map.insert (colName `T.append` T.pack "_count") aggColumn groupKeyColumns
                in
                    DataFrame finalDfMap

-- | Extracts a single DFValue from a DataFrame, assuming it has one column and one row.
extractSingleValue :: DataFrame -> DFValue
extractSingleValue (DataFrame dfMap) =
    if Map.null dfMap || V.null (snd . head . Map.toList $ dfMap)
        then NA
        else V.head (snd . head . Map.toList $ dfMap)

-- | Helper to convert DFValue to Text for column names
valueToText :: DFValue -> T.Text
valueToText (TextValue t) = t
valueToText v = T.pack (show v) -- Fallback for other DFValue types

-- | Creates a pivot table from a DataFrame.
pivotTable :: T.Text -> T.Text -> T.Text -> (T.Text -> GroupedDataFrame -> DataFrame) -> DataFrame -> DataFrame
pivotTable rowKeyCol colKeyCol valueCol aggFunc df =
    let
        -- Get all rows from the original DataFrame
        allRows = toRows df

        -- Extract all unique values for row and column keys
        uniqueRowValues = nub $ map (\r -> Map.findWithDefault NA rowKeyCol r) allRows
        uniqueColValues = nub $ map (\r -> Map.findWithDefault NA colKeyCol r) allRows

        -- Create the pivot table structure
        -- Each column in the pivot table corresponds to a unique colKeyCol value
        pivotColumns = Map.fromList $ map (\cVal ->
            (valueToText cVal, V.fromList $ map (\rVal ->
                let
                    -- Filter the original DataFrame to get rows matching current rVal and cVal
                    filteredDf = filterRows (
                        \row -> Map.findWithDefault NA rowKeyCol row == rVal &&
                                Map.findWithDefault NA colKeyCol row == cVal
                        ) df
                    -- Group the filtered DataFrame by the rowKeyCol (or just pass it directly if no further grouping needed)
                    groupedFilteredDf = groupBy [rowKeyCol] filteredDf -- Grouping by rowKeyCol for consistency with aggFunc
                    -- Apply the aggregation function to the grouped data
                    aggregatedResultDf = aggFunc valueCol groupedFilteredDf
                in
                    extractSingleValue aggregatedResultDf
            ) uniqueRowValues)
            ) uniqueColValues

        -- Add the row key column to the pivot table
        rowKeyColumn = V.fromList uniqueRowValues
        finalPivotDfMap = Map.insert rowKeyCol rowKeyColumn pivotColumns
    in
        DataFrame finalPivotDfMap