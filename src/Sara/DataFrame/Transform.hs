module Sara.DataFrame.Transform (
    selectColumns,
    addColumn,
    melt,
    applyColumn,
    mutate
) where

import Sara.DSL.MutateParser (parseExpr, evaluate)
import Control.Parallel.Strategies as Parallel (withStrategy, rseq, parList, using, Strategy)
import Data.Vector.Strategies (parVector)
import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.Types (DataFrame(..), Column, Row, toRows, DFValue(..))

-- | Selects a subset of columns from a DataFrame.
-- If a column name does not exist, it will be ignored.
selectColumns :: [T.Text] -> DataFrame -> DataFrame
selectColumns cols (DataFrame dfMap) =
    let
        selectedMap = Map.filterWithKey (\k _ -> k `elem` cols) dfMap
    in
        DataFrame selectedMap

-- | Adds a new column to a DataFrame or modifies an existing one.
-- The new column's values are computed by applying a function to each row.
addColumn :: T.Text -> (Row -> DFValue) -> DataFrame -> DataFrame
addColumn colName f (DataFrame dfMap) =
    let
        rows = toRows (DataFrame dfMap)
        newColumnValues = V.fromList $ map f rows
        updatedDfMap = Map.insert colName newColumnValues dfMap
    in
        DataFrame updatedDfMap

-- | Unpivots a DataFrame from wide format to long format.
-- 'id_vars' are columns to remain as identifier variables.
-- 'value_vars' are columns to unpivot.
melt :: [T.Text] -> [T.Text] -> DataFrame -> DataFrame
melt id_vars value_vars (DataFrame dfMap) =
    let
        rows = toRows (DataFrame dfMap)
        
        -- Function to process a single row
        processRow :: Row -> [Row]
        processRow row = 
            let
                -- Extract id_vars from the current row
                idValues = Map.filterWithKey (\k _ -> k `elem` id_vars) row
            in
                -- For each value_var, create a new row
                concatMap (\valVar ->
                    case Map.lookup valVar row of
                        Just val -> [Map.union idValues (Map.fromList [(T.pack "variable", TextValue valVar), (T.pack "value", val)])]
                        Nothing -> [] -- Skip if value_var not found in row
                ) value_vars

        -- Process all rows and flatten the list of lists of rows
        meltedRows = concatMap processRow rows

        -- Convert melted rows back to DataFrame
        newDfMap = if null meltedRows
                   then Map.empty
                   else
                       let
                           colNames = Map.keys (head meltedRows)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- meltedRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap

-- | Applies a function to a specified column in a DataFrame.
parallelMapVector :: (DFValue -> DFValue) -> V.Vector DFValue -> V.Vector DFValue
parallelMapVector f vec = V.fromList $ withStrategy (parList rseq) (V.toList $ V.map f vec)

-- | Applies a function to a specified column in a DataFrame.
applyColumn :: T.Text -> (DFValue -> DFValue) -> DataFrame -> DataFrame
applyColumn colName f (DataFrame dfMap) =
    case Map.lookup colName dfMap of
        Just col ->
            let updatedCol = parallelMapVector f col
            in DataFrame (Map.insert colName updatedCol dfMap)
        Nothing ->
            DataFrame dfMap -- Column not found, return original DataFrame

-- | Adds new columns or modifies existing ones based on DSL expressions.
mutate :: DataFrame -> [(T.Text, T.Text)] -> DataFrame
mutate df [] = df
mutate df ((newColName, exprStr):exprs) =
    let
        rows = toRows df
        newDf = case parseExpr exprStr of
            Left err -> df -- Or handle error more gracefully
            Right expr ->
                let
                    newColumnValues = V.fromList $ map (evaluate expr) rows
                    updatedDfMap = Map.insert newColName newColumnValues (case df of DataFrame m -> m)
                in
                    DataFrame updatedDfMap
    in
        mutate newDf exprs