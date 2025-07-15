{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

-- | This module provides functions for handling missing data in a `DataFrame`.
-- Missing data is represented by the `NA` constructor of the `DFValue` type.
module Sara.DataFrame.Missing (
    -- * Filling Missing Values
    fillna,
    ffill,
    bfill,
    -- * Dropping Missing Values
    dropna,
    DropAxis(..),
    -- * Checking for Missing Values
    isna,
    notna
) where

import Sara.DataFrame.Types (DFValue(..), DataFrame(..), Column, toRows, fromRows, KnownColumns, CanBeDFValue(..))
import Data.Typeable (Typeable)
import Data.Proxy (Proxy(..))
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

-- | Fills `NA` values in a `DataFrame` with a specified value.
-- If a column name is provided, only that column is filled.
-- Otherwise, all columns are filled.
-- The fill value must be of a type that can be converted to a `DFValue`.
fillna :: forall a cols. (KnownColumns cols, Typeable a, CanBeDFValue a) => DataFrame cols -> Proxy a -> Maybe T.Text -> a -> DataFrame cols
fillna (DataFrame dfMap) _ colNameM fillVal = DataFrame $ Map.mapWithKey (\k col ->
    case colNameM of
        Just cn | cn == k -> V.map (\x -> if x == NA then toDFValue fillVal else x) col
        Nothing -> V.map (\x -> if x == NA then toDFValue fillVal else x) col
        _ -> col
    ) dfMap

-- | Forward fills `NA` values in a `DataFrame`.
-- For each column, `NA` values are replaced with the last non-`NA` value.
ffill :: KnownColumns cols => DataFrame cols -> DataFrame cols
ffill (DataFrame dfMap) = DataFrame $ Map.map ffillColumn dfMap
  where
    ffillColumn :: Column -> Column
    ffillColumn col = V.postscanl' (\lastVal currentVal -> if currentVal == NA then lastVal else currentVal) NA col

-- | Backward fills `NA` values in a `DataFrame`.
-- For each column, `NA` values are replaced with the next non-`NA` value.
bfill :: KnownColumns cols => DataFrame cols -> DataFrame cols
bfill (DataFrame dfMap) = DataFrame $ Map.map bfillColumn dfMap
  where
    bfillColumn :: Column -> Column
    bfillColumn col = V.reverse $ V.postscanl' (\lastVal currentVal -> if currentVal == NA then lastVal else currentVal) NA (V.reverse col)

-- | Specifies the axis along which to drop `NA` values.
data DropAxis = DropRows | DropColumns
    deriving (Show, Eq)

-- | Drops rows or columns containing `NA` values.
--
-- *   `DropRows`: Drops rows that contain `NA` values.
-- *   `DropColumns`: Drops columns that contain `NA` values.
--
-- The `thresholdM` parameter specifies the minimum number of non-`NA` values required to keep a row or column.
-- If `Nothing`, the row or column is dropped if it contains any `NA` values.
dropna :: KnownColumns cols => DataFrame cols -> DropAxis -> Maybe Int -> DataFrame cols
dropna df DropRows thresholdM =
    let rows = toRows df
        filteredRows = filter (\row ->
            let nonNACount = length $ filter (/= NA) (Map.elems row)
            in case thresholdM of
                Just t -> nonNACount >= t
                Nothing -> not (NA `elem` (Map.elems row)) -- Keep if no NA
            ) rows
    in fromRows filteredRows
dropna (DataFrame dfMap) DropColumns thresholdM =
    let filteredMap = Map.filter (\col ->
            let nonNACount = length $ filter (/= NA) (V.toList col)
            in case thresholdM of
                Just t -> nonNACount >= t
                Nothing -> not (NA `V.elem` col) -- Keep if no NA
            ) dfMap
    in DataFrame filteredMap

-- | Returns a `DataFrame` of `BoolValue`s indicating whether each element is `NA`.
isna :: KnownColumns cols => DataFrame cols -> DataFrame cols
isna (DataFrame dfMap) = DataFrame $ Map.map (V.map (BoolValue . (== NA))) dfMap

-- | Returns a `DataFrame` of `BoolValue`s indicating whether each element is not `NA`.
notna :: KnownColumns cols => DataFrame cols -> DataFrame cols
notna (DataFrame dfMap) = DataFrame $ Map.map (V.map (BoolValue . (/= NA))) dfMap
