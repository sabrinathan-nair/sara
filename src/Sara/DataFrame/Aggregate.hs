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
{-# LANGUAGE GADTs #-}

-- | This module provides functions for grouping and aggregating data in a DataFrame.
-- It allows for type-safe operations like `groupBy`, `sumAgg`, `meanAgg`, and `countAgg`.
module Sara.DataFrame.Aggregate (
    -- * Types
    GroupedDataFrame,
    -- * Functions
    groupBy,
    sumAgg,
    meanAgg,
    countAgg,
    -- * Typeclasses
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

-- | A `GroupedDataFrame` is the result of a `groupBy` operation.
-- It's a map where keys are `TypeLevelRow`s representing the unique values of grouping columns,
-- and values are `DataFrame`s containing the corresponding rows.
type GroupedDataFrame (groupCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)]) = Map (TypeLevelRow groupCols) (DataFrame originalCols)



-- | Groups a `DataFrame` by a list of columns.
--
-- >>> :set -XDataKinds
-- >>> let df = fromRows @'["name" ::: T.Text, "age" ::: Int] [Map.fromList [("name", TextValue "Alice"), ("age", IntValue 25)], Map.fromList [("name", TextValue "Bob"), ("age", IntValue 30)], Map.fromList [("name", TextValue "Alice"), ("age", IntValue 35)]]
-- >>> let grouped = groupBy @'["name" ::: T.Text] df
-- >>> Map.size grouped
-- 2
groupBy :: forall (groupCols :: [Symbol]) (cols :: [(Symbol, Type)])
        . (HasColumns groupCols cols, KnownColumns (SymbolsToSchema groupCols cols)) => DataFrame cols -> GroupedDataFrame (SymbolsToSchema groupCols cols) cols
groupBy df = 
    let
        rows = toRows df
        getGroupKey :: Row -> TypeLevelRow (SymbolsToSchema groupCols cols)
        getGroupKey row = toTypeLevelRow @(SymbolsToSchema groupCols cols) row

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

-- | A type family to generate a new column name for an aggregation.
-- For example, `AggColName "age" "sum"` becomes `"age_sum"`.
type family AggColName (col :: Symbol) (op :: Symbol) :: Symbol where
    AggColName col op = AppendSymbol (AppendSymbol col "_") op

-- | Represents the type of aggregation to perform.
data AggOp = Sum | Mean | Count


-- | A typeclass for types that can be aggregated.
-- It connects an `AggOp` with the underlying data type and the aggregation logic.
class (CanBeDFValue a, CanBeDFValue b) => Aggregatable (op :: AggOp) a b where
    -- | The core aggregation function.
    aggregateOp :: Proxy op -> V.Vector a -> b

-- Sum instances
instance Aggregatable 'Sum Int Double where
    aggregateOp _ v = fromIntegral (V.sum v)

instance Aggregatable 'Sum Double Double where
    aggregateOp _ = V.sum

-- Mean instances
instance Aggregatable 'Mean Int Double where
    aggregateOp _ v | V.null v = 0.0
                    | otherwise = fromIntegral (V.sum v) / fromIntegral (V.length v)

instance Aggregatable 'Mean Double Double where
    aggregateOp _ v | V.null v = 0.0
                    | otherwise = V.sum v / fromIntegral (V.length v)

-- Count instance
instance CanBeDFValue a => Aggregatable 'Count a Int where
    aggregateOp _ = V.length

-- | Aggregates a `GroupedDataFrame` by summing the values in a specified column.
-- The resulting `DataFrame` contains the grouping columns and a new column with the aggregated values.
-- The new column is named by appending "_sum" to the original column name.
sumAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol,Type)]) a b newAggCol outCols.
          ( HasColumn aggCol cols
          , KnownColumns groupCols
          , a ~ TypeOf aggCol cols
          , b ~ Double
          , Aggregatable 'Sum a b
          , newAggCol ~ AggColName aggCol "sum"
          , outCols ~ Nub (Append groupCols '[ '(newAggCol, b)])
          , KnownColumns outCols
          , KnownSymbol newAggCol
          , CanBeDFValue b
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
                    Just col -> V.mapMaybe (fromDFValue @a) col
                    Nothing -> V.empty

                aggResult :: b
                aggResult = aggregateOp (Proxy @'Sum) aggColVector
            in
                Map.insert newAggColName (toDFValue aggResult) groupKey

        newRows = Map.elems $ Map.mapWithKey processGroup groupedDf
    in
        fromRows newRows

-- | Aggregates a `GroupedDataFrame` by calculating the mean of the values in a specified column.
-- The resulting `DataFrame` contains the grouping columns and a new column with the aggregated values.
-- The new column is named by appending "_mean" to the original column name.
meanAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol,Type)]) a b newAggCol outCols.
           ( HasColumn aggCol cols
           , KnownColumns groupCols
           , a ~ TypeOf aggCol cols
           , b ~ Double
           , Aggregatable 'Mean a b
           , newAggCol ~ AggColName aggCol "mean"
           , outCols ~ Nub (Append groupCols '[ '(newAggCol, b)])
           , KnownColumns outCols
           , KnownSymbol newAggCol
           , CanBeDFValue b
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
                    Just col -> V.mapMaybe (fromDFValue @a) col
                    Nothing -> V.empty

                aggResult :: b
                aggResult = aggregateOp (Proxy @'Mean) aggColVector
            in
                Map.insert newAggColName (toDFValue aggResult) groupKey

        newRows = Map.elems $ Map.mapWithKey processGroup groupedDf
    in
        fromRows newRows

-- | Aggregates a `GroupedDataFrame` by counting the non-NA values in a specified column.
-- The resulting `DataFrame` contains the grouping columns and a new column with the aggregated values.
-- The new column is named by appending "_count" to the original column name.
countAgg :: forall (aggCol :: Symbol) (groupCols :: [(Symbol, Type)]) (cols :: [(Symbol,Type)]) a b newAggCol outCols.
            ( HasColumn aggCol cols
            , KnownColumns groupCols
            , a ~ TypeOf aggCol cols
            , b ~ Int
            , Aggregatable 'Count a b
            , newAggCol ~ AggColName aggCol "count"
            , outCols ~ Nub (Append groupCols '[ '(newAggCol, b)])
            , KnownColumns outCols
            , KnownSymbol newAggCol
            , CanBeDFValue b
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
                    Just col -> V.mapMaybe (fromDFValue @a) col
                    Nothing -> V.empty

                aggResult :: b
                aggResult = aggregateOp (Proxy @'Count) aggColVector
            in
                Map.insert newAggColName (toDFValue aggResult) groupKey

        newRows = Map.elems $ Map.mapWithKey processGroup groupedDf
    in
        fromRows newRows