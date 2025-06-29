{-# LANGUAGE OverloadedStrings #-}

module Sara.DataFrame.Statistics (
    -- General statistical functions
    sumV,
    meanV,
    stdV,
    minV,
    maxV,
    countV,
    rollingApply
) where

import Sara.DataFrame.Types
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Data.List (foldl')

-- Helper to convert DFValue to Double for calculations, treating NA as 0 or skipping
toNumeric :: DFValue -> Maybe Double
toNumeric (IntValue i) = Just $ fromIntegral i
toNumeric (DoubleValue d) = Just d
toNumeric _ = Nothing

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

-- | Applies a function over a rolling window of a column.
rollingApply :: DataFrame -> Int -> String -> (V.Vector DFValue -> DFValue) -> DataFrame
rollingApply (DataFrame dfMap) window colName aggFunc =
    let col = dfMap Map.! (T.pack colName)
        rollingCol = V.fromList $ map (\i ->
            let windowed = V.slice i (min window (V.length col - i)) col
            in aggFunc windowed
            ) [0 .. V.length col - 1]
        newDfMap = Map.insert (T.pack $ colName ++ "_rolling") rollingCol dfMap
    in DataFrame newDfMap
