{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

module Sara.DataFrame.Statistics (
    -- General statistical functions
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
    -- Rolling window functions
    rollingApply
) where

import Sara.DataFrame.Types
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Data.List (foldl', sort, group, sortBy)
import Data.Ord (comparing)

-- Helper to convert DFValue to Double for calculations, treating NA as 0 or skipping
toNumeric :: DFValue -> Maybe Double
toNumeric (IntValue i) = Just $ fromIntegral i
toNumeric (DoubleValue d) = Just d
toNumeric _ = Nothing

-- Helper to convert DFValue to Double, treating NA as 0
toDouble :: DFValue -> Double
toDouble (IntValue i) = fromIntegral i
toDouble (DoubleValue d) = d
toDouble _ = 0.0

-- | Calculates the sum of a vector of DFValues.
sumV :: V.Vector DFValue -> DFValue
sumV vec = 
    let numericVals = V.mapMaybe toNumeric vec
    in if V.null numericVals
       then NA
       else DoubleValue $ V.sum numericVals

-- | Calculates the mean of a vector of DFValues.
meanV :: V.Vector DFValue -> DFValue
meanV vec = 
    let numericVals = V.mapMaybe toNumeric vec
        len = V.length numericVals
    in if len == 0
       then NA
       else DoubleValue $ V.sum numericVals / fromIntegral len

-- | Calculates the standard deviation of a vector of DFValues.
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

-- | Calculates the minimum of a vector of DFValues.
minV :: V.Vector DFValue -> DFValue
minV vec = 
    let numericVals = V.mapMaybe toNumeric vec
    in if V.null numericVals
       then NA
       else DoubleValue $ V.minimum numericVals

-- | Calculates the maximum of a vector of DFValues.
maxV :: V.Vector DFValue -> DFValue
maxV vec = 
    let numericVals = V.mapMaybe toNumeric vec
    in if V.null numericVals
       then NA
       else DoubleValue $ V.maximum numericVals

-- | Counts the non-NA values in a vector of DFValues.
countV :: V.Vector DFValue -> DFValue
countV vec = IntValue $ V.length $ V.filter (/= NA) vec

-- | Calculates the median of a vector of DFValues.
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

-- | Calculates the mode of a vector of DFValues.
modeV :: V.Vector DFValue -> DFValue
modeV vec =
    let nonNAValues = V.filter (/= NA) vec
    in if V.null nonNAValues
       then NA
       else
           let grouped = group (V.toList nonNAValues)
               sortedGroups = sortBy (comparing (length)) grouped
           in head (last sortedGroups)

-- | Calculates the variance of a vector of DFValues.
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

-- | Calculates the skewness of a vector of DFValues.
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

-- | Calculates the kurtosis of a vector of DFValues.
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
rollingApply :: KnownColumns cols => DataFrame cols -> Int -> String -> (V.Vector DFValue -> DFValue) -> DataFrame cols
rollingApply (DataFrame dfMap) window colName aggFunc =
    let col = dfMap Map.! (T.pack colName)
        rollingCol = V.fromList $ map (\i ->
            let windowed = V.slice i (min window (V.length col - i)) col
            in aggFunc windowed
            ) [0 .. V.length col - 1]
        newDfMap = Map.insert ((T.pack colName) `T.append` "_rolling") rollingCol dfMap
    in DataFrame newDfMap