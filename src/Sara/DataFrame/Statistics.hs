{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators #-}

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
    countValues,
    -- * Quantile Calculation
    quantile,
    -- * Percentile Calculation
    percentile,
    -- * Correlation Calculation
    correlate,
    -- * Covariance Calculation
    covariance,
    -- * Summary Statistics
    summaryStatistics,
    -- * Expanding Window Functions
    expandingApply,
    -- * Exponentially Weighted Moving Functions
    ewmApply
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

-- | Calculates the specified quantile (e.g., 0.25 for Q1, 0.5 for median, 0.75 for Q3) of a numeric column.
-- Non-numeric values and NA values are ignored.
-- The quantile value `q` must be between 0.0 and 1.0 (inclusive).
-- Returns `Left SaraError` if the column is not found, is empty after filtering, or `q` is out of range.
quantile :: forall (col :: Symbol) cols.
            ( KnownSymbol col
            , HasColumn col cols
            , CanBeDFValue (TypeOf col cols)
            ) => Proxy col -> Double -> DataFrame cols -> Either SaraError DFValue
quantile p q df
    | q < 0.0 || q > 1.0 = Left $ InvalidArgument "Quantile value must be between 0.0 and 1.0."
    | otherwise = do
        let colName = T.pack $ symbolVal p
        case Map.lookup colName (getDataFrameMap df) of
            Nothing -> Left $ ColumnNotFound colName
            Just colVector ->
                let numericVals = V.mapMaybe toNumeric colVector
                    len = V.length numericVals
                in if len == 0
                   then Left $ EmptyColumn colName "Cannot calculate quantile for an empty column."
                   else
                       let sortedVals = sort (V.toList numericVals)
                           -- Linear interpolation for quantile calculation
                           -- (N - 1) * q
                           index = fromIntegral (len - 1) * q
                           -- Floor and ceiling indices
                           idxFloor = floor index
                           idxCeil = ceiling index
                       in if idxFloor == idxCeil
                          then Right $ DoubleValue (sortedVals !! idxFloor)
                          else
                              let v1 = sortedVals !! idxFloor
                                  v2 = sortedVals !! idxCeil
                                  -- Interpolate: v1 + (v2 - v1) * (index - idxFloor)
                                  interpolated = v1 + (v2 - v1) * (index - fromIntegral idxFloor)
                              in Right $ DoubleValue interpolated

-- | Calculates the specified percentile (e.g., 25 for 25th percentile, 50 for median, 75 for 75th percentile) of a numeric column.
-- Non-numeric values and NA values are ignored.
-- The percentile value `p` must be between 0.0 and 100.0 (inclusive).
-- Returns `Left SaraError` if the column is not found, is empty after filtering, or `p` is out of range.
percentile :: forall (col :: Symbol) cols.
              ( KnownSymbol col
              , HasColumn col cols
              , CanBeDFValue (TypeOf col cols)
              ) => Proxy col -> Double -> DataFrame cols -> Either SaraError DFValue
percentile p_col p_val df
    | p_val < 0.0 || p_val > 100.0 = Left $ InvalidArgument "Percentile value must be between 0.0 and 100.0."
    | otherwise = quantile p_col (p_val / 100.0) df

-- | Calculates the Pearson correlation coefficient between two numeric columns.
-- Non-numeric values and NA values are ignored.
-- Returns `Left SaraError` if columns are not found, are not numeric, or have insufficient data.
correlate :: forall (col1 :: Symbol) (col2 :: Symbol) cols.
             ( KnownSymbol col1
             , KnownSymbol col2
             , HasColumn col1 cols
             , HasColumn col2 cols
             , CanBeDFValue (TypeOf col1 cols)
             , CanBeDFValue (TypeOf col2 cols)
             ) => Proxy col1 -> Proxy col2 -> DataFrame cols -> Either SaraError Double
correlate p1 p2 df = do
    let colName1 = T.pack $ symbolVal p1
    let colName2 = T.pack $ symbolVal p2

    colVector1 <- case Map.lookup colName1 (getDataFrameMap df) of
        Nothing -> Left $ ColumnNotFound colName1
        Just vec -> Right vec

    colVector2 <- case Map.lookup colName2 (getDataFrameMap df) of
        Nothing -> Left $ ColumnNotFound colName2
        Just vec -> Right vec

    let numericVals1 = V.mapMaybe toNumeric colVector1
    let numericVals2 = V.mapMaybe toNumeric colVector2

    -- Filter out NAs from both vectors simultaneously based on their indices
    let filteredVals = V.zip numericVals1 numericVals2

    let len = V.length filteredVals
    if len < 2
       then Left $ InvalidArgument "Correlation requires at least 2 data points after filtering NAs."
       else
           let xVals = V.map fst filteredVals
               yVals = V.map snd filteredVals

               meanX = V.sum xVals / fromIntegral len
               meanY = V.sum yVals / fromIntegral len

               stdX = sqrt (V.sum (V.map (\x -> (x - meanX) * (x - meanX)) xVals) / fromIntegral (len - 1))
               stdY = sqrt (V.sum (V.map (\y -> (y - meanY) * (y - meanY)) yVals) / fromIntegral (len - 1))

           in if stdX == 0.0 || stdY == 0.0
              then Left $ InvalidArgument "Cannot calculate correlation: standard deviation is zero for one or both columns."
              else
                  let covar = V.sum (V.zipWith (\x y -> (x - meanX) * (y - meanY)) xVals yVals) / fromIntegral (len - 1)
                  in Right $ covar / (stdX * stdY)

-- | Calculates the covariance between two numeric columns.
-- Non-numeric values and NA values are ignored.
-- Returns `Left SaraError` if columns are not found, are not numeric, or have insufficient data.
covariance :: forall (col1 :: Symbol) (col2 :: Symbol) cols.
              ( KnownSymbol col1
              , KnownSymbol col2
              , HasColumn col1 cols
              , HasColumn col2 cols
              , CanBeDFValue (TypeOf col1 cols)
              , CanBeDFValue (TypeOf col2 cols)
              ) => Proxy col1 -> Proxy col2 -> DataFrame cols -> Either SaraError Double
covariance p1 p2 df = do
    let colName1 = T.pack $ symbolVal p1
    let colName2 = T.pack $ symbolVal p2

    colVector1 <- case Map.lookup colName1 (getDataFrameMap df) of
        Nothing -> Left $ ColumnNotFound colName1
        Just vec -> Right vec

    colVector2 <- case Map.lookup colName2 (getDataFrameMap df) of
        Nothing -> Left $ ColumnNotFound colName2
        Just vec -> Right vec

    let numericVals1 = V.mapMaybe toNumeric colVector1
    let numericVals2 = V.mapMaybe toNumeric colVector2

    let filteredVals = V.zip numericVals1 numericVals2

    let len = V.length filteredVals
    if len < 2
       then Left $ InvalidArgument "Covariance requires at least 2 data points after filtering NAs."
       else
           let xVals = V.map fst filteredVals
               yVals = V.map snd filteredVals

               meanX = V.sum xVals / fromIntegral len
               meanY = V.sum yVals / fromIntegral len

               covarianceVal = V.sum (V.zipWith (\x y -> (x - meanX) * (y - meanY)) xVals yVals) / fromIntegral (len - 1)
           in Right covarianceVal

-- | Generates descriptive statistics for numeric columns in a DataFrame.
-- For each numeric column, it calculates count, mean, standard deviation, min, 25th percentile (Q1), median (50th percentile), 75th percentile (Q3), and max.
-- Non-numeric columns are ignored.
-- Returns a Map where keys are statistic names (e.g., "count", "mean"), and values are Maps from column names to their calculated statistic.
summaryStatistics :: forall cols.
                     ( KnownColumns cols
                     ) => DataFrame cols -> Map.Map T.Text (Map.Map T.Text DFValue)
summaryStatistics df = 
    let dfMap = getDataFrameMap df

        -- Helper to get numeric values from a column vector
        getNumericVals :: T.Text -> V.Vector Double
        getNumericVals colName = V.mapMaybe toNumeric (dfMap Map.! colName)

        -- Calculate statistics for a single numeric column
        calculateColumnStats :: T.Text -> Map.Map T.Text DFValue
        calculateColumnStats colName =
            let numericVals = getNumericVals colName
                len = V.length numericVals
            in if len == 0
               then Map.fromList [
                   ("count", IntValue 0),
                   ("mean", NA),
                   ("std", NA),
                   ("min", NA),
                   ("25%", NA),
                   ("50%", NA),
                   ("75%", NA),
                   ("max", NA)
                   ]
               else
                   let sortedVals = sort (V.toList numericVals)
                       -- Inline quantile calculation for 25%, 50%, 75%
                       calcQuantile :: Double -> DFValue
                       calcQuantile q =
                           let index = fromIntegral (len - 1) * q
                               idxFloor = floor index
                               idxCeil = ceiling index
                           in if idxFloor == idxCeil
                              then DoubleValue (sortedVals !! idxFloor)
                              else
                                  let v1 = sortedVals !! idxFloor
                                      v2 = sortedVals !! idxCeil
                                      interpolated = v1 + (v2 - v1) * (index - fromIntegral idxFloor)
                                  in DoubleValue interpolated
                   in Map.fromList [
                       ("count", IntValue len),
                       ("mean", meanV (V.map DoubleValue numericVals)),
                       ("std", stdV (V.map DoubleValue numericVals)),
                       ("min", minV (V.map DoubleValue numericVals)),
                       ("25%", calcQuantile 0.25),
                       ("50%", calcQuantile 0.50),
                       ("75%", calcQuantile 0.75),
                       ("max", maxV (V.map DoubleValue numericVals))
                       ]

        -- Filter for numeric columns and calculate their statistics
        numericColumnStats = Map.fromList $ map (\colName -> (colName, calculateColumnStats colName)) $ filter (\cn -> Map.member cn dfMap && not (V.null (V.mapMaybe toNumeric (dfMap Map.! cn)))) (Map.keys dfMap)

        -- Restructure the map: StatisticName -> (ColumnName -> Value)
        restructuredMap = foldl' (\acc (colName, statsMap) ->
            foldl' (\acc' (statName, statVal) ->
                Map.insertWith Map.union statName (Map.singleton colName statVal) acc'
            ) acc (Map.toList statsMap)
            ) Map.empty (Map.toList numericColumnStats)

    in restructuredMap

-- | Applies a function over an expanding window of a column.
-- The new column is named by appending "_expanding" to the original column name.
-- The aggregation function should operate on a `V.Vector DFValue` and return a `DFValue`.
expandingApply :: forall (col :: Symbol) cols.
                  ( KnownSymbol col
                  , HasColumn col cols
                  , KnownColumns (cols :++: '[ '(col, TypeOf col cols)]) -- Schema for the output DataFrame
                  , CanBeDFValue (TypeOf col cols)
                  ) => Proxy col -> (V.Vector DFValue -> DFValue) -> DataFrame cols -> Either SaraError (DataFrame (cols :++: '[ '(col, TypeOf col cols)]))
expandingApply p aggFunc df = do
    let colName = T.pack $ symbolVal p
    case Map.lookup colName (getDataFrameMap df) of
        Nothing -> Left $ ColumnNotFound colName
        Just colVector ->
            let expandingCol = V.generate (V.length colVector) (\i ->
                    let windowed = V.slice 0 (i + 1) colVector
                    in aggFunc windowed
                    )
                newColName = colName `T.append` "_expanding"
                newDfMap = Map.insert newColName expandingCol (getDataFrameMap df)
            in Right $ DataFrame newDfMap

-- | Applies an Exponentially Weighted Moving Average (EWMA) to a numeric column.
-- The new column is named by appending "_ewma" to the original column name.
-- The `alpha` parameter is the smoothing factor, typically between 0 and 1.
-- Returns `Left SaraError` if the column is not found, is not numeric, or `alpha` is out of range.
ewmApply :: forall (col :: Symbol) cols.
             ( KnownSymbol col
             , HasColumn col cols
             , KnownColumns (cols :++: '[ '(col, TypeOf col cols)])
             , CanBeDFValue (TypeOf col cols)
             ) => Proxy col -> Double -> DataFrame cols -> Either SaraError (DataFrame (cols :++: '[ '(col, TypeOf col cols)]))
ewmApply p alpha df
    | alpha <= 0.0 || alpha >= 1.0 = Left $ InvalidArgument "Alpha for EWMA must be between 0.0 and 1.0 (exclusive)."
    | otherwise = do
        let colName = T.pack $ symbolVal p
        case Map.lookup colName (getDataFrameMap df) of
            Nothing -> Left $ ColumnNotFound colName
            Just colVector ->
                let numericVals = V.mapMaybe toNumeric colVector
                    ewmaVals = V.scanl' (\prevEwma currentVal -> alpha * currentVal + (1 - alpha) * prevEwma) (V.head numericVals) (V.tail numericVals)
                    newColName = colName `T.append` "_ewma"
                    newDfMap = Map.insert newColName (V.map DoubleValue ewmaVals) (getDataFrameMap df)
                in Right $ DataFrame newDfMap