{-# LANGUAGE OverloadedStrings #-}

module Sara.DataFrame.TimeSeries (
    resample,
    ResampleRule(..),
    shift,
    pctChange,
    fromRows
) where

import Sara.DataFrame.Types
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Data.Time
import Data.Time.Calendar (toGregorian, fromGregorian)
import Data.List (foldl')

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

-- | The rule for resampling a time series.
data ResampleRule = Daily | Monthly | Yearly
    deriving (Show, Eq)

-- | Parse a 'DFValue' into a 'UTCTime' if possible.
parseUTCTime :: DFValue -> Maybe UTCTime
parseUTCTime (TimestampValue t) = Just t
parseUTCTime (TextValue t) = parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" (show t)
parseUTCTime _ = Nothing

-- | Create a DataFrame from a list of rows.
fromRows :: [Row] -> DataFrame
fromRows [] = DataFrame Map.empty
fromRows rows@(firstRow:_) =
    let columns = Map.keys firstRow
        colMap = Map.fromList $ map (\colName -> (colName, V.fromList $ map (Map.! colName) rows)) columns
    in DataFrame colMap

-- | Group the rows of a DataFrame by a given time period.
groupByTime :: DataFrame -> String -> ResampleRule -> Map.Map Day [Row]
groupByTime df timeColumn rule = foldl' reducer Map.empty (toRows df)
  where
    reducer acc row = 
        let key = truncateTime (row Map.! (T.pack timeColumn))
        in Map.insertWith (++) key [row] acc

    truncateTime :: DFValue -> Day
    truncateTime (TimestampValue t) = case rule of
        Daily -> utctDay t
        Monthly -> let (y, m, _) = toGregorian $ utctDay t in fromGregorian y m 1
        Yearly -> let (y, _, _) = toGregorian $ utctDay t in fromGregorian y 1 1
    truncateTime _ = error "Time column must be of type TimestampValue"

-- | Resample a DataFrame based on a time column.
resample :: DataFrame -> String -> ResampleRule -> (V.Vector DFValue -> DFValue) -> DataFrame
resample df timeColumn rule aggFunc =
    let grouped = groupByTime df timeColumn rule
        aggData = Map.map (\rows ->
            let dfFromRows = fromRows rows
            in applyAgg dfFromRows
            ) grouped
        newDf = concatDFs $ Map.elems aggData
    in newDf
    where
        applyAgg :: DataFrame -> DataFrame
        applyAgg (DataFrame dfMap) =
            DataFrame $ Map.map (\col -> V.singleton $ aggFunc col) dfMap

        concatDFs :: [DataFrame] -> DataFrame
        concatDFs dfs = DataFrame $ Map.unionsWith (V.++) $ map (\(DataFrame m) -> m) dfs

-- | Shift the values in a column by a given number of periods.
shift :: DataFrame -> String -> Int -> DataFrame
shift (DataFrame dfMap) colName periods =
    let col = dfMap Map.! (T.pack colName)
        shiftedCol = if periods > 0
            then V.concat [V.replicate periods NA, V.take (V.length col - periods) col]
            else V.concat [V.drop (abs periods) col, V.replicate (abs periods) NA]
        newDfMap = Map.insert (T.pack $ colName ++ "_shifted") shiftedCol dfMap
    in DataFrame newDfMap

-- | Calculate the percentage change between the current and a prior element.
pctChange :: DataFrame -> String -> DataFrame
pctChange (DataFrame dfMap) colName =
    let col = dfMap Map.! (T.pack colName)
        pctChangeCol = V.zipWith (\current previous ->
            case (toDouble current, toDouble previous) of
                (c, p) -> DoubleValue $ (c - p) / p
            ) (V.drop 1 col) (V.take (V.length col - 1) col)
        newDfMap = Map.insert (T.pack $ colName ++ "_pct_change") (NA `V.cons` pctChangeCol) dfMap
    in DataFrame newDfMap
    where
        toDouble :: DFValue -> Double
        toDouble (IntValue i) = fromIntegral i
        toDouble (DoubleValue d) = d
        toDouble _ = 0.0
