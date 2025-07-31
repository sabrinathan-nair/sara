{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}

-- | This module provides a collection of statistical functions that can be applied to `DataFrame` columns.
-- These functions operate on `V.Vector DFValue` and return a single `DFValue` as the result.
module Sara.DataFrame.Statistics (
    -- * General Statistical Functions
    sumV,
    meanV,
    stdV,
    minV,
    maxV,
    countV,
    medianV,
    modeV,
    varianceV,
    skewV,
    kurtosisV,
    -- * Rolling Window Functions
    rollingApply,
    -- * Value Counting
    countValues
) where

import Sara.DataFrame.Types
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Data.List (sort, group, sortBy)
import Data.Ord (comparing)
import GHC.TypeLits (Symbol, KnownSymbol, symbolVal)
import Data.Proxy (Proxy(..))
import Sara.Error (SaraError(..))

-- | Converts a `DFValue` to a `Double` for calculations.
-- Returns `Nothing` if the value is not numeric.
toNumeric :: DFValue -> Maybe Double
toNumeric (IntValue i) = Just $ fromIntegral i
toNumeric (DoubleValue d) = Just d
toNumeric _ = Nothing

-- | Calculates the sum of a vector of `DFValue`s.
-- Non-numeric values are ignored.
sumV :: V.Vector DFValue -> DFValue
sumV vec = 
    let numericVals = V.mapMaybe toNumeric vec
    in if V.null numericVals
       then NA
       else DoubleValue $ V.sum numericVals

-- | Calculates the mean of a vector of `DFValue`s.
-- Non-numeric values are ignored.
meanV :: V.Vector DFValue -> DFValue
meanV vec = 
    let numericVals = V.mapMaybe toNumeric vec
        len = V.length numericVals
    in if len == 0
       then NA
       else DoubleValue $ V.sum numericVals / fromIntegral len

-- | Calculates the standard deviation of a vector of `DFValue`s.
-- Non-numeric values are ignored.
stdV :: V.Vector DFValue -> DFValue
stdV vec = 
    let numericVals = V.mapMaybe toNumeric vec
        len = V.length numericVals
    in if len < 2
       then NA -- Standard deviation requires at least 2 data points
       else
           let m = V.sum numericVals / fromIntegral len
               sumSqDiff = V.sum $ V.map (\x -> (x - m) * (x - m)) numericVals
           in DoubleValue $ sqrt (sumSqDiff / fromIntegral (len - 1))

-- | Calculates the minimum of a vector of `DFValue`s.
-- Non-numeric values are ignored.
minV :: V.Vector DFValue -> DFValue
minV vec = 
    let numericVals = V.mapMaybe toNumeric vec
    in if V.null numericVals
       then NA
       else DoubleValue $ V.minimum numericVals

-- | Calculates the maximum of a vector of `DFValue`s.
-- Non-numeric values are ignored.
maxV :: V.Vector DFValue -> DFValue
maxV vec = 
    let numericVals = V.mapMaybe toNumeric vec
    in if V.null numericVals
       then NA
       else DoubleValue $ V.maximum numericVals

-- | Counts the non-`NA` values in a vector of `DFValue`s.
countV :: V.Vector DFValue -> DFValue
countV vec = IntValue $ V.length $ V.filter (/= NA) vec

-- | Calculates the median of a vector of `DFValue`s.
-- Non-numeric values are ignored.
medianV :: V.Vector DFValue -> DFValue
medianV vec =
    let numericVals = V.mapMaybe toNumeric vec
        sortedVals = sort (V.toList numericVals)
        len = length sortedVals
    in if len == 0
       then NA
       else if odd len
            then DoubleValue $ sortedVals !! (len `div` 2)
            else DoubleValue $ (sortedVals !! (len `div` 2 - 1) + sortedVals !! (len `div` 2)) / 2.0

-- | Calculates the mode of a vector of `DFValue`s.
-- The mode is the value that appears most frequently.
modeV :: V.Vector DFValue -> DFValue
modeV vec =
    let nonNAValues = V.filter (/= NA) vec
    in if V.null nonNAValues
       then NA
       else
           let grouped = group (V.toList nonNAValues)
               -- Sort by length (frequency) in descending order
               sortedGroups = sortBy (comparing (negate . length)) grouped
                      in case sortedGroups of
               [] -> NA
               (firstGroup:_) -> case firstGroup of
                   [] -> NA
                   (modeVal:_) -> modeVal

-- | Calculates the variance of a vector of `DFValue`s.
-- Non-numeric values are ignored.
varianceV :: V.Vector DFValue -> DFValue
varianceV vec =
    let numericVals = V.mapMaybe toNumeric vec
        len = V.length numericVals
    in if len < 2
       then NA
       else
           let m = V.sum numericVals / fromIntegral len
               sumSqDiff = V.sum $ V.map (\x -> (x - m) * (x - m)) numericVals
           in DoubleValue $ sumSqDiff / fromIntegral (len - 1)

-- | Calculates the skewness of a vector of `DFValue`s.
-- Skewness is a measure of the asymmetry of the probability distribution of a real-valued random variable about its mean.
skewV :: V.Vector DFValue -> DFValue
skewV vec =
    let numericVals = V.mapMaybe toNumeric vec
        len = V.length numericVals
    in if len < 3
       then NA
       else
           let m = V.sum numericVals / fromIntegral len
               s = sqrt (V.sum (V.map (\x -> (x - m) * (x - m)) numericVals) / fromIntegral (len - 1))
               sumCubedDiff = V.sum $ V.map (\x -> (x - m) ** 3) numericVals
           in DoubleValue $ (fromIntegral len / ((fromIntegral len - 1) * (fromIntegral len - 2))) * (sumCubedDiff / (s ** 3))

-- | Calculates the kurtosis of a vector of `DFValue`s.
-- Kurtosis is a measure of the "tailedness" of the probability distribution of a real-valued random variable.
kurtosisV :: V.Vector DFValue -> DFValue
kurtosisV vec =
    let numericVals = V.mapMaybe toNumeric vec
        len = V.length numericVals
    in if len < 4
       then NA
       else
           let m = V.sum numericVals / fromIntegral len
               s = sqrt (V.sum (V.map (\x -> (x - m) * (x - m)) numericVals) / fromIntegral (len - 1))
               sumFourthDiff = V.sum $ V.map (\x -> (x - m) ** 4) numericVals
           in DoubleValue $ (fromIntegral len * (fromIntegral len + 1) / ((fromIntegral len - 1) * (fromIntegral len - 2) * (fromIntegral len - 3))) * (sumFourthDiff / (s ** 4)) - (3 * (fromIntegral len - 1) * (fromIntegral len - 1) / ((fromIntegral len - 2) * (fromIntegral len - 3)))

-- | Applies a function over a rolling window of a column.
-- The new column is named by appending "_rolling" to the original column name.
rollingApply :: KnownColumns cols => DataFrame cols -> Int -> String -> (V.Vector DFValue -> DFValue) -> DataFrame cols
rollingApply (DataFrame dfMap) window colName aggFunc =
    let col = dfMap Map.! T.pack colName
        rollingCol = V.fromList $ map (\i ->
            let windowed = V.slice i (min window (V.length col - i)) col
            in aggFunc windowed
            ) [0 .. V.length col - 1]
        newDfMap = Map.insert (T.pack colName `T.append` "_rolling") rollingCol dfMap
    in DataFrame newDfMap

-- | Counts the occurrences of unique, non-NA values in a specified column.
-- Returns a new DataFrame with two columns: the unique values and their counts.
-- The resulting DataFrame is sorted by count in descending order.
--
-- Example:
-- >>> let df = fromRows @'["Category" ::: T.Text] [Map.fromList [("Category", TextValue "A")], Map.fromList [("Category", TextValue "B")], Map.fromList [("Category", TextValue "A")], Map.fromList [("Category", NA)]]
-- >>> countValues (Proxy @"Category") df
-- DataFrame {dfMap = fromList [("Category",[TextValue "A",TextValue "B"]),("Count",[IntValue 2,IntValue 1])]}
countValues :: forall (col :: Symbol) cols.
               ( KnownSymbol col
               , HasColumn col cols
               , KnownColumns '[ '(col, TypeOf col cols), '("Count", Int)]
               , CanBeDFValue (TypeOf col cols)
               ) => Proxy col -> DataFrame cols -> Either SaraError (DataFrame '[ '(col, TypeOf col cols), '("Count", Int)])
countValues p df = do
    let colName = T.pack $ symbolVal p
    case Map.lookup colName (getDataFrameMap df) of
        Nothing -> Left $ ColumnNotFound colName
        Just colVector ->
            let nonNAValues = V.filter (/= NA) colVector
                -- Group and count frequencies
                counts = Map.fromListWith (+) [(v, 1) | v <- V.toList nonNAValues]
                -- Convert to a list of (value, count) pairs and sort by count descending
                sortedCounts = sortBy (comparing (negate . snd)) (Map.toList counts)
                -- Create new columns for the result DataFrame
                valueCol = V.fromList $ map fst sortedCounts
                countCol = V.fromList $ map (IntValue . snd) sortedCounts
                -- Construct the new DataFrame
                resultMap = Map.fromList [(colName, valueCol), ("Count", countCol)]
            in Right $ DataFrame resultMap