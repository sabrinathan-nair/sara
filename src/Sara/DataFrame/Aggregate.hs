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
import Data.List (foldl')
import Sara.DataFrame.Types
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import Data.Maybe (fromJust)
import Data.Kind (Type)

-- | A DataFrame grouped by certain columns.
-- The outer Map's keys are the unique combinations of values from the grouping columns (represented as a Row),
-- and the values are DataFrames containing the rows belonging to that group.
type GroupedDataFrame (groupCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)]) = Map Row (DataFrame originalCols)

type family SymbolsToSchema (syms :: [Symbol]) (originalSchema :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    SymbolsToSchema '[] _ = '[]
    SymbolsToSchema (s ': ss) originalSchema = '(s, TypeOf s originalSchema) ': SymbolsToSchema ss originalSchema

-- | Groups a DataFrame by the specified column names.
groupBy :: forall (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol, Type)]).
          (HasColumns (MapSymbols groupCols) cols, KnownColumns groupCols)
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
sumAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol, Type)]) (outCols :: [(Symbol, Type)]) (newAggCol :: Symbol) a.
          ( HasColumn aggCol cols, KnownColumns groupCols
          , newAggCol ~ AggColName aggCol "sum", KnownSymbol newAggCol
          , outCols ~ Nub (Append groupCols '[ '(newAggCol, Double)]), KnownColumns outCols
          , CanBeDFValue a, CanAggregate a, TypeOf aggCol cols ~ a
          )
       => GroupedDataFrame groupCols cols -> DataFrame outCols
sumAgg groupedDf =
    let
        aggColName = T.pack (symbolVal (Proxy @aggCol))
        aggFunc col = DoubleValue $ V.sum $ V.map (toAggDouble @a . fromJust . fromDFValue @a) col
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
meanAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol, Type)]) (outCols :: [(Symbol, Type)]) (newAggCol :: Symbol) a.
           ( HasColumn aggCol cols, KnownColumns groupCols
           , newAggCol ~ AggColName aggCol "mean", KnownSymbol newAggCol
           , outCols ~ Nub (Append groupCols '[ '(newAggCol, Double)]), KnownColumns outCols
           , CanBeDFValue a, CanAggregate a, TypeOf aggCol cols ~ a
           )
        => GroupedDataFrame groupCols cols -> DataFrame outCols
meanAgg groupedDf =
    let
        aggColName = T.pack (symbolVal (Proxy @aggCol))
        aggFunc col =
            let
                validValues = V.catMaybes $ V.map (fromDFValue @a) col
                (total, count) = V.foldl' (\(accSum, accCount) x -> (accSum + toAggDouble @a x, accCount + 1 :: Int)) (0.0, 0 :: Int) validValues
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
countAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol, Type)]) (outCols :: [(Symbol, Type)]) (newAggCol :: Symbol) a.
            ( HasColumn aggCol cols, KnownColumns groupCols
            , newAggCol ~ AggColName aggCol "count", KnownSymbol newAggCol
            , outCols ~ Nub (Append groupCols '[ '(newAggCol, Int)]), KnownColumns outCols
            , CanBeDFValue a, CanAggregate a, TypeOf aggCol cols ~ a
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