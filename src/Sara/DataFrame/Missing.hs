{-# LANGUAGE OverloadedStrings #-}

module Sara.DataFrame.Missing (
    fillna,
    ffill,
    bfill,
    dropna,
    DropAxis(..)
) where

import Sara.DataFrame.Types (DFValue(..), DataFrame(..), Column, Row, toRows)
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Data.List (foldl')
import Sara.DataFrame.TimeSeries (fromRows)

-- | Fills NA values in a DataFrame with a specified DFValue.
fillna :: DataFrame -> Maybe T.Text -> DFValue -> DataFrame
fillna (DataFrame dfMap) colNameM fillVal = DataFrame $ Map.mapWithKey (\k col ->
    case colNameM of
        Just cn | cn == k -> V.map (\x -> if x == NA then fillVal else x) col
        Nothing -> V.map (\x -> if x == NA then fillVal else x) col
        _ -> col
    ) dfMap

-- | Forward fills NA values in a DataFrame.
ffill :: DataFrame -> DataFrame
ffill (DataFrame dfMap) = DataFrame $ Map.map ffillColumn dfMap
  where
    ffillColumn :: Column -> Column
    ffillColumn col = V.postscanl' (\lastVal currentVal -> if currentVal == NA then lastVal else currentVal) NA col

-- | Backward fills NA values in a DataFrame.
bfill :: DataFrame -> DataFrame
bfill (DataFrame dfMap) = DataFrame $ Map.map bfillColumn dfMap
  where
    bfillColumn :: Column -> Column
    bfillColumn col = V.reverse $ V.postscanl' (\lastVal currentVal -> if currentVal == NA then lastVal else currentVal) NA (V.reverse col)

-- | Drops rows or columns containing NA values.
dropna :: DataFrame -> DropAxis -> Maybe Int -> DataFrame
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

data DropAxis = DropRows | DropColumns
    deriving (Show, Eq)
