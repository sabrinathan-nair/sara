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
{-# LANGUAGE UndecidableInstances #-}

module Sara.DataFrame.Aggregate (
    GroupedDataFrame,
    groupBy,
    sumAgg,
    meanAgg,
    countAgg
) where

import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.List (foldl', nub)
import Sara.DataFrame.Types
import Sara.DataFrame.Wrangling (filterRows)
import GHC.TypeLits
import Data.Proxy (Proxy(..))

-- | A DataFrame grouped by certain columns.
-- The outer Map's keys are the unique combinations of values from the grouping columns (represented as a Row),
-- and the values are DataFrames containing the rows belonging to that group.
type GroupedDataFrame (groupCols :: [Symbol]) (originalCols :: [Symbol]) = Map Row (DataFrame originalCols)

-- | Groups a DataFrame by the specified column names.
groupBy :: forall (groupCols :: [Symbol]) (cols :: [Symbol]).
          (HasColumns groupCols cols, KnownColumns groupCols)
          => DataFrame cols -> GroupedDataFrame groupCols cols
groupBy df =
    let
        rows = toRows df
        groupColNames = columnNames (Proxy @groupCols)
        getGroupKey :: Row -> Row
        getGroupKey row = Map.filterWithKey (\k _ -> k `elem` groupColNames) row

        initialGroupedMap = Map.empty
        groupedMap = foldl' (\accMap row ->
            let
                groupKey = getGroupKey row
                singleRowDf = DataFrame $ Map.map V.singleton row
            in
                Map.insertWith (\(DataFrame newMap) (DataFrame existingMap) ->
                    DataFrame $ Map.unionWith (V.++) existingMap newMap
                ) groupKey singleRowDf accMap
            ) initialGroupedMap rows
    in
        groupedMap

-- | Type family to create a new column name for an aggregation.
type family AggColName (col :: Symbol) (op :: Symbol) :: Symbol where
    AggColName col op = AppendSymbol (AppendSymbol col "_") op

-- | Aggregates a GroupedDataFrame by summing a specified column.
sumAgg :: forall (aggCol :: Symbol) (groupCols :: [Symbol]) (cols :: [Symbol]) (outCols :: [Symbol]) (newAggCol :: Symbol).
          ( HasColumn aggCol cols, KnownColumns groupCols
          , newAggCol ~ AggColName aggCol "sum", KnownSymbol newAggCol
          , outCols ~ Nub (Append groupCols '[newAggCol]), KnownColumns outCols
          )
       => GroupedDataFrame groupCols cols -> DataFrame outCols
sumAgg groupedDf =
    let
        aggColName = T.pack (symbolVal (Proxy @aggCol))
        aggFunc col = DoubleValue $ V.sum $ V.map toDouble col
            where toDouble (IntValue i) = fromIntegral i
                  toDouble (DoubleValue d) = d
                  toDouble _ = 0.0
        aggregatedRows = Map.map (\(DataFrame dfMap) ->
            case Map.lookup aggColName dfMap of
                Just col -> aggFunc col
                Nothing -> NA -- Should not happen due to HasColumn constraint
            ) groupedDf

        groupColNames = columnNames (Proxy @groupCols)
        newAggColName = T.pack (symbolVal (Proxy @newAggCol))

        newDfMap = if Map.null aggregatedRows
            then Map.empty
            else
                let
                    groupKeys = Map.keys aggregatedRows
                    groupKeyColumns = Map.fromList $ map (\name ->
                        (name, V.fromList $ map (\keyRow -> Map.findWithDefault NA name keyRow) groupKeys)
                        ) groupColNames
                    aggColumn = V.fromList $ Map.elems aggregatedRows
                in
                    Map.insert newAggColName aggColumn groupKeyColumns
    in
        DataFrame newDfMap

-- | Aggregates a GroupedDataFrame by calculating the mean of a specified column.
meanAgg :: forall (aggCol :: Symbol) (groupCols :: [Symbol]) (cols :: [Symbol]) (outCols :: [Symbol]) (newAggCol :: Symbol).
           ( HasColumn aggCol cols, KnownColumns groupCols
           , newAggCol ~ AggColName aggCol "mean", KnownSymbol newAggCol
           , outCols ~ Nub (Append groupCols '[newAggCol]), KnownColumns outCols
           )
        => GroupedDataFrame groupCols cols -> DataFrame outCols
meanAgg groupedDf =
    let
        aggColName = T.pack (symbolVal (Proxy @aggCol))
        aggFunc col =
            let (total, count) = V.foldl' (\(accSum, accCount) val ->
                    case val of
                        IntValue i -> (accSum + fromIntegral i, accCount + 1)
                        DoubleValue d -> (accSum + d, accCount + 1)
                        _ -> (accSum, accCount)
                    ) (0.0, 0) col
            in if count > 0 then DoubleValue (total / fromIntegral count) else NA
        aggregatedRows = Map.map (\(DataFrame dfMap) ->
            case Map.lookup aggColName dfMap of
                Just col -> aggFunc col
                Nothing -> NA -- Should not happen due to HasColumn constraint
            ) groupedDf

        groupColNames = columnNames (Proxy @groupCols)
        newAggColName = T.pack (symbolVal (Proxy @newAggCol))

        newDfMap = if Map.null aggregatedRows
            then Map.empty
            else
                let
                    groupKeys = Map.keys aggregatedRows
                    groupKeyColumns = Map.fromList $ map (\name ->
                        (name, V.fromList $ map (\keyRow -> Map.findWithDefault NA name keyRow) groupKeys)
                        ) groupColNames
                    aggColumn = V.fromList $ Map.elems aggregatedRows
                in
                    Map.insert newAggColName aggColumn groupKeyColumns
    in
        DataFrame newDfMap

-- | Aggregates a GroupedDataFrame by counting non-NA values in a specified column.
countAgg :: forall (aggCol :: Symbol) (groupCols :: [Symbol]) (cols :: [Symbol]) (outCols :: [Symbol]) (newAggCol :: Symbol).
            ( HasColumn aggCol cols, KnownColumns groupCols
            , newAggCol ~ AggColName aggCol "count", KnownSymbol newAggCol
            , outCols ~ Nub (Append groupCols '[newAggCol]), KnownColumns outCols
            )
         => GroupedDataFrame groupCols cols -> DataFrame outCols
countAgg groupedDf =
    let
        aggColName = T.pack (symbolVal (Proxy @aggCol))
        aggFunc col = IntValue $ V.length $ V.filter (/= NA) col
        aggregatedRows = Map.map (\(DataFrame dfMap) ->
            case Map.lookup aggColName dfMap of
                Just col -> aggFunc col
                Nothing -> NA -- Should not happen due to HasColumn constraint
            ) groupedDf

        groupColNames = columnNames (Proxy @groupCols)
        newAggColName = T.pack (symbolVal (Proxy @newAggCol))

        newDfMap = if Map.null aggregatedRows
            then Map.empty
            else
                let
                    groupKeys = Map.keys aggregatedRows
                    groupKeyColumns = Map.fromList $ map (\name ->
                        (name, V.fromList $ map (\keyRow -> Map.findWithDefault NA name keyRow) groupKeys)
                        ) groupColNames
                    aggColumn = V.fromList $ Map.elems aggregatedRows
                in
                    Map.insert newAggColName aggColumn groupKeyColumns
    in
        DataFrame newDfMap

-- | Extracts a single DFValue from a DataFrame, assuming it has one column and one row.
extractSingleValue :: DataFrame cols -> DFValue
extractSingleValue (DataFrame dfMap) =
    if Map.null dfMap || V.null (snd . head . Map.toList $ dfMap)
        then NA
        else V.head (snd . head . Map.toList $ dfMap)

-- | Helper to convert DFValue to Text for column names
valueToText :: DFValue -> T.Text
valueToText (TextValue t) = t
valueToText v = T.pack (show v) -- Fallback for other DFValue types


