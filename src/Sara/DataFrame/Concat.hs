{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

-- | This module provides functions for concatenating DataFrames.
module Sara.DataFrame.Concat (
    concatDF
) where

import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.Types (DataFrame(..), ConcatAxis(..), KnownColumns)

-- | Concatenates a list of `DataFrame`s along a specified `ConcatAxis`.
--
-- __ConcatRows__:
--
-- *   All `DataFrame`s must have the same columns (schema).
-- *   The resulting `DataFrame` will contain all rows from the input `DataFrame`s.
--
-- __ConcatColumns__:
--
-- *   All `DataFrame`s must have the same number of rows.
-- *   The resulting `DataFrame` will contain all columns from the input `DataFrame`s.
-- *   If there are overlapping column names, the columns from `DataFrame`s later in the list will overwrite earlier ones.
--
-- >>> :set -XDataKinds
-- >>> let df1 = fromRows @'["name" ::: T.Text, "age" ::: Int] [Map.fromList [("name", TextValue "Alice"), ("age", IntValue 25)]]
-- >>> let df2 = fromRows @'["name" ::: T.Text, "age" ::: Int] [Map.fromList [("name", TextValue "Bob"), ("age", IntValue 30)]]
-- >>> let df3 = concatDF ConcatRows [df1, df2]
-- >>> toRows df3
-- [fromList [("age",IntValue 25),("name",TextValue "Alice")],fromList [("age",IntValue 30),("name",TextValue "Bob")]]
concatDF :: KnownColumns cols => ConcatAxis -> [DataFrame cols] -> DataFrame cols
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