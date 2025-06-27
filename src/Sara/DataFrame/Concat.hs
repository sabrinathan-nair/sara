module Sara.DataFrame.Concat (
    concatDF
) where

import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.Types (DataFrame(..), Column, Row, DFValue(..), ConcatAxis(..), toRows)

-- | Concatenates a list of DataFrames along a specified axis.
--
-- For row-wise concatenation (ConcatRows):
--   - DataFrames must have the same columns.
--   - Rows are appended.
--
-- For column-wise concatenation (ConcatColumns):
--   - DataFrames must have the same number of rows.
--   - Columns are appended.
--   - If column names overlap, later DataFrames' columns will overwrite earlier ones.
concatDF :: ConcatAxis -> [DataFrame] -> DataFrame
concatDF _ [] = DataFrame Map.empty
concatDF ConcatRows dfs =
    let
        -- Assuming all DataFrames have the same columns for row-wise concat
        -- We'll take the columns from the first DataFrame as the reference
        (DataFrame firstDfMap) = head dfs
        columnNames = Map.keys firstDfMap

        -- Aggregate all columns from all DataFrames
        concatenatedColumns = Map.fromList $ map (\colName ->
            let
                allColValues = V.concat [ col | (DataFrame dfMap) <- dfs, Just col <- [Map.lookup colName dfMap] ]
            in
                (colName, allColValues)
            ) columnNames
    in
        DataFrame concatenatedColumns

concatDF ConcatColumns dfs =
    let
        -- For column-wise concat, we simply union the maps of columns
        -- If there are overlapping column names, the columns from DataFrames
        -- later in the list will overwrite earlier ones.
        mergedDfMap = foldl Map.union Map.empty [dfMap | (DataFrame dfMap) <- dfs]
    in
        DataFrame mergedDfMap
