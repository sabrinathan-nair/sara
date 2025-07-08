{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

module Sara.DataFrame.TimeSeries (
    resample,
    ResampleRule(..),
    shift,
    pctChange,
    fromRows,
    groupByTime,
    rollingApply
) where

import Sara.DataFrame.Types (DataFrame(..), Row, DFValue(..), KnownColumns(..), toRows, fromRows, HasColumn, TypeOf, CanAggregate(..), CanBeDFValue(..))
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Data.Time
import Data.Time.Calendar (toGregorian, fromGregorian)
import Data.List (foldl')
import Data.Proxy (Proxy(..))
import GHC.TypeLits (Symbol, KnownSymbol, symbolVal)

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



-- | Group the rows of a DataFrame by a given time period.
groupByTime :: forall (timeCol :: Symbol) cols.
              (KnownSymbol timeCol, HasColumn timeCol cols, TypeOf timeCol cols ~ UTCTime, KnownColumns cols)
              => Proxy timeCol -> ResampleRule -> DataFrame cols -> Map.Map Day [Row]
groupByTime timeColProxy rule (DataFrame dfMap) = foldl' reducer Map.empty (toRows (DataFrame dfMap :: DataFrame cols))
  where
    timeColName = T.pack (symbolVal timeColProxy)
    reducer acc row =
        let (TimestampValue t) = row Map.! timeColName -- Guaranteed to be TimestampValue by TypeOf constraint
            key = truncateTime t
        in Map.insertWith (++) key [row] acc

    truncateTime :: UTCTime -> Day
    truncateTime t =
        case rule of
            Daily -> utctDay t
            Monthly -> let (y, m, _) = toGregorian $ utctDay t in fromGregorian y m 1
            Yearly -> let (y, _, _) = toGregorian $ utctDay t in fromGregorian y 1 1

-- | Resample a DataFrame based on a time column.
resample :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols, TypeOf col cols ~ UTCTime) => Proxy col -> ResampleRule -> (V.Vector DFValue -> DFValue) -> DataFrame cols -> DataFrame cols
resample colProxy rule aggFunc (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
        grouped = groupByTime colProxy rule (DataFrame dfMap :: DataFrame cols)
        aggData = Map.map (\rows ->
            let dfFromRows = fromRows rows
            in applyAgg dfFromRows
            ) grouped
        newDf = concatDFs $ Map.elems aggData

    in newDf
    where
        applyAgg :: DataFrame cols -> DataFrame cols
        applyAgg (DataFrame dfMap') =
            DataFrame $ Map.map (V.singleton . aggFunc) dfMap'

        concatDFs :: [DataFrame cols] -> DataFrame cols
        concatDFs dfs = DataFrame $ Map.unionsWith (V.++) $ map (\(DataFrame m) -> m) dfs

-- | Shift the values in a column by a given number of periods.
shift :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> Int -> DataFrame cols -> DataFrame cols
shift colProxy periods (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
        col = dfMap Map.! colName
        shiftedCol = if periods > 0
            then V.concat [V.replicate periods NA, V.take (V.length col - periods) col]
            else V.concat [V.drop (abs periods) col, V.replicate (abs periods) NA]
        newDfMap = Map.insert (T.pack $ T.unpack colName ++ "_shifted") shiftedCol dfMap
    in DataFrame newDfMap

-- | Calculate the percentage change between the current and a prior element.
pctChange :: forall (col :: Symbol) cols a.
              (KnownSymbol col, HasColumn col cols, KnownColumns cols, TypeOf col cols ~ a, CanAggregate a, CanBeDFValue a)
              => Proxy col -> DataFrame cols -> DataFrame cols
pctChange colProxy (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
        col = dfMap Map.! colName
        pctChangeCol = V.zipWith (\current previous ->
            case (fromDFValue @a current, fromDFValue @a previous) of
                (Just c, Just p) -> DoubleValue $ (toAggDouble c - toAggDouble p) / toAggDouble p
                _ -> NA
            ) (V.drop 1 col) (V.take (V.length col - 1) col)
        newDfMap = Map.insert (T.pack $ T.unpack colName ++ "_pct_change") (NA `V.cons` pctChangeCol) dfMap
    in DataFrame newDfMap

-- | Apply a rolling window function to a column.
rollingApply :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> Int -> (V.Vector DFValue -> DFValue) -> DataFrame cols -> DataFrame cols
rollingApply colProxy windowSize aggFunc (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
        col = dfMap Map.! colName
        rollingCol = V.generate (V.length col) (\i ->
            if i < windowSize - 1
                then NA
                else aggFunc $ V.slice (i - windowSize + 1) windowSize col
            )
        newDfMap = Map.insert (T.pack $ T.unpack colName ++ "_rolling") rollingCol dfMap
    in DataFrame newDfMap