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
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}

module Sara.DataFrame.Aggregate (
    GroupedDataFrame,
    groupBy,
    sumAgg,
    meanAgg,
    countAgg,
    AggOp(..),
    AggregationResult,
    Aggregatable
) where

import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.List (foldl')
import Sara.DataFrame.Types
import GHC.TypeLits
import Data.Proxy (Proxy(..))

import Data.Kind (Type)

-- | A DataFrame grouped by certain columns.
-- The outer Map's keys are the unique combinations of values from the grouping columns (represented as a TypeLevelRow),
-- and the values are DataFrames containing the rows belonging to that group.
type GroupedDataFrame (groupCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)]) = Map (TypeLevelRow groupCols) (DataFrame originalCols)

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
        getGroupKey :: Row -> TypeLevelRow groupCols
        getGroupKey row = toTypeLevelRow @groupCols row

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

-- | A data type to represent an aggregation operation.
data AggOp = Sum | Mean | Count

-- | A type family to determine the result type of an aggregation.
type family AggregationResult (op :: AggOp) (a :: Type) :: Type where
    AggregationResult 'Sum Int = Int
    AggregationResult 'Sum Double = Double
    AggregationResult 'Mean Int = Double
    AggregationResult 'Mean Double = Double
    AggregationResult 'Count a = Int

-- | A type class for aggregatable types.
class (CanBeDFValue a, CanBeDFValue (AggregationResult op a)) => Aggregatable (op :: AggOp) (a :: Type) where
    aggregateOp :: Proxy op -> V.Vector a -> AggregationResult op a

instance Aggregatable 'Sum Int where
    aggregateOp _ = V.sum

instance Aggregatable 'Sum Double where
    aggregateOp _ = V.sum

instance Aggregatable 'Mean Int where
    aggregateOp _ v = fromIntegral (V.sum v) / fromIntegral (V.length v)

instance Aggregatable 'Mean Double where
    aggregateOp _ v = V.sum v / fromIntegral (V.length v)

instance CanBeDFValue a => Aggregatable 'Count a where
    aggregateOp _ = V.length

-- | Aggregates a GroupedDataFrame by summing a specified column.
sumAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol,Type)]) a newAggCol outCols.
          ( HasColumn aggCol cols
          , KnownColumns groupCols
          , a ~ TypeOf aggCol cols
          , Aggregatable 'Sum a
          , newAggCol ~ AggColName aggCol "sum"
          , outCols ~ Nub (Append groupCols '[ '(newAggCol, AggregationResult 'Sum a)])
          , KnownColumns outCols
          , KnownSymbol newAggCol
          , CanBeDFValue (AggregationResult 'Sum a)
          )
       => GroupedDataFrame groupCols cols -> DataFrame outCols
sumAgg groupedDf =
    let
        aggColName = T.pack $ symbolVal (Proxy @aggCol)
        newAggColName = T.pack $ symbolVal (Proxy @newAggCol)

        processGroup :: TypeLevelRow groupCols -> DataFrame cols -> Row
        processGroup (TypeLevelRow groupKey) (DataFrame dfMap) =
            let
                aggColVector = case Map.lookup aggColName dfMap of
                    Just col -> V.catMaybes $ V.map (fromDFValue @a) col
                    Nothing -> V.empty

                aggResult = aggregateOp (Proxy @'Sum) aggColVector
            in
                Map.insert newAggColName (toDFValue aggResult) groupKey

        newRows = Map.elems $ Map.mapWithKey processGroup groupedDf
    in
        fromRows newRows

-- | Aggregates a GroupedDataFrame by calculating the mean of a specified column.
meanAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol,Type)]) a newAggCol outCols.
           ( HasColumn aggCol cols
           , KnownColumns groupCols
           , a ~ TypeOf aggCol cols
           , Aggregatable 'Mean a
           , newAggCol ~ AggColName aggCol "mean"
           , outCols ~ Nub (Append groupCols '[ '(newAggCol, AggregationResult 'Mean a)])
           , KnownColumns outCols
           , KnownSymbol newAggCol
           , CanBeDFValue (AggregationResult 'Mean a)
           )
        => GroupedDataFrame groupCols cols -> DataFrame outCols
meanAgg groupedDf =
    let
        aggColName = T.pack $ symbolVal (Proxy @aggCol)
        newAggColName = T.pack $ symbolVal (Proxy @newAggCol)

        processGroup :: TypeLevelRow groupCols -> DataFrame cols -> Row
        processGroup (TypeLevelRow groupKey) (DataFrame dfMap) =
            let
                aggColVector = case Map.lookup aggColName dfMap of
                    Just col -> V.catMaybes $ V.map (fromDFValue @a) col
                    Nothing -> V.empty

                aggResult = aggregateOp (Proxy @'Mean) aggColVector
            in
                Map.insert newAggColName (toDFValue aggResult) groupKey

        newRows = Map.elems $ Map.mapWithKey processGroup groupedDf
    in
        fromRows newRows

-- | Aggregates a GroupedDataFrame by counting non-NA values in a specified column.
countAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol,Type)]) a newAggCol outCols.
            ( HasColumn aggCol cols
            , KnownColumns groupCols
            , a ~ TypeOf aggCol cols
            , Aggregatable 'Count a
            , newAggCol ~ AggColName aggCol "count"
            , outCols ~ Nub (Append groupCols '[ '(newAggCol, AggregationResult 'Count a)])
            , KnownColumns outCols
            , KnownSymbol newAggCol
            , CanBeDFValue (AggregationResult 'Count a)
            )
         => GroupedDataFrame groupCols cols -> DataFrame outCols
countAgg groupedDf =
    let
        aggColName = T.pack $ symbolVal (Proxy @aggCol)
        newAggColName = T.pack $ symbolVal (Proxy @newAggCol)

        processGroup :: TypeLevelRow groupCols -> DataFrame cols -> Row
        processGroup (TypeLevelRow groupKey) (DataFrame dfMap) =
            let
                aggColVector = case Map.lookup aggColName dfMap of
                    Just col -> V.catMaybes $ V.map (fromDFValue @a) col
                    Nothing -> V.empty

                aggResult = aggregateOp (Proxy @'Count) aggColVector
            in
                Map.insert newAggColName (toDFValue aggResult) groupKey

        newRows = Map.elems $ Map.mapWithKey processGroup groupedDf
    in
        fromRows newRows