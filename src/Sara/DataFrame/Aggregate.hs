{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

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
import Streaming (Stream, Of(..))
import qualified Streaming.Prelude as S





-- | A typeclass for aggregatable types.
class Aggregatable a where
    -- | The result type of the aggregation.
    type Aggregated a :: Type
    -- | The aggregation function.
    aggregate :: [a] -> Aggregated a

instance Aggregatable Int where
    type Aggregated Int = Double
    aggregate = fromIntegral . sum

instance Aggregatable Double where
    type Aggregated Double = Double
    aggregate = sum

instance Aggregatable T.Text where
    type Aggregated T.Text = Int
    aggregate = length

-- | Sums the values of a column in a grouped `DataFrame`.
sumAgg :: forall (col :: Symbol) (groupCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)])
       . ( HasColumn col originalCols
         , Aggregatable (TypeOf col originalCols)
         , KnownSymbol col
         , KnownColumns groupCols
         , KnownColumns originalCols
         , CanBeDFValue (TypeOf col originalCols)
         , CanBeDFValue (Aggregated (TypeOf col originalCols))
         , KnownColumns (groupCols :++: '[ '(col, Aggregated (TypeOf col originalCols))])
         ) => IO (GroupedDataFrame groupCols originalCols) -> IO (DataFrame (groupCols :++: '[ '(col, Aggregated (TypeOf col originalCols))]))
sumAgg groupedDfIO = do
    groupedDf <- groupedDfIO
    let newRows = Map.foldrWithKey (\key (DataFrame df) acc ->
            let
                colName = T.pack $ symbolVal (Proxy @col)
                valsEither = case Map.lookup colName df of
                    Just vec -> traverse (fromDFValue @(TypeOf col originalCols)) (V.toList vec)
                    Nothing -> Right []
            in
                case valsEither of
                    Right vals ->
                        let aggVal = (aggregate :: [TypeOf col originalCols] -> Aggregated (TypeOf col originalCols)) vals
                        in (Map.insert (colName <> T.pack "_sum") (toDFValue aggVal) (let (TypeLevelRow r) = key in r)) : acc
                    Left _ -> acc -- Or handle error appropriately
            ) [] groupedDf
    return $ fromRows newRows

-- | Calculates the mean of a column in a grouped `DataFrame`.
meanAgg :: forall (col :: Symbol) (groupCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)])
        . ( HasColumn col originalCols
          , Aggregatable (TypeOf col originalCols)
          , KnownSymbol col
          , Fractional (Aggregated (TypeOf col originalCols))
          , KnownColumns groupCols
          , KnownColumns originalCols
          , CanBeDFValue (TypeOf col originalCols)
          , CanBeDFValue (Aggregated (TypeOf col originalCols))
          , KnownColumns (groupCols :++: '[ '(col, Aggregated (TypeOf col originalCols))])
          ) => IO (GroupedDataFrame groupCols originalCols) -> IO (DataFrame (groupCols :++: '[ '(col, Aggregated (TypeOf col originalCols))]))
meanAgg groupedDfIO = do
    groupedDf <- groupedDfIO
    return $ fromRows (newRows groupedDf)
  where
    newRows groupedDf = Map.foldrWithKey (\key (DataFrame df) acc ->
        let
            colName = T.pack $ symbolVal (Proxy @col)
            valsEither = case Map.lookup colName df of
                Just vec -> traverse (fromDFValue @(TypeOf col originalCols)) (V.toList vec)
                Nothing -> Right []
        in
            case valsEither of
                Right vals ->
                    let aggVal = (aggregate :: [TypeOf col originalCols] -> Aggregated (TypeOf col originalCols)) vals
                        countVal = fromIntegral $ length vals
                    in (Map.insert (colName <> T.pack "_mean") (toDFValue (aggVal / countVal)) (let (TypeLevelRow r) = key in r)) : acc
                Left _ -> acc -- Or handle error appropriately
        ) [] groupedDf

-- | Counts the values in a single group.
countGroup :: forall (col :: Symbol) (groupCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)])
           . ( HasColumn col originalCols
             , KnownSymbol col
             , KnownColumns groupCols
             , KnownColumns originalCols
             , CanBeDFValue (TypeOf col originalCols)
             , KnownColumns (groupCols :++: '[ '(col, Int)])
             ) => GroupedDataFrame groupCols originalCols -> DataFrame (groupCols :++: '[ '(col, Int)])
countGroup groupedDf = fromRows newRows
  where
    newRows = Map.foldrWithKey (\key (DataFrame df) acc ->
        let
            colName = T.pack $ symbolVal (Proxy @col)
            -- Just count the number of elements in the vector for that column
            countVal = case Map.lookup colName df of
                Just vec -> V.length vec
                Nothing -> 0
        in
            (Map.insert (colName <> T.pack "_count") (toDFValue countVal) (let (TypeLevelRow r) = key in r)) : acc
        ) [] groupedDf

-- | Counts the values of a column in a grouped `DataFrame`.
countAgg :: forall (col :: Symbol) (groupCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)])
         . ( HasColumn col originalCols
           , KnownSymbol col
           , KnownColumns groupCols
           , KnownColumns originalCols
           , CanBeDFValue (TypeOf col originalCols)
           , KnownColumns (groupCols :++: '[ '(col, Int)])
           ) => IO (GroupedDataFrame groupCols originalCols) -> IO (DataFrame (groupCols :++: '[ '(col, Int)]))
countAgg groupedDfIO = do
    groupedDf <- groupedDfIO
    return $ countGroup @col groupedDf

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
        . (HasColumns groupCols cols, KnownColumns (SymbolsToSchema groupCols cols)) => Stream (Of (DataFrame cols)) IO () -> IO (GroupedDataFrame (SymbolsToSchema groupCols cols) cols)
groupBy =
    S.foldM_ (\accMap df -> do
        let rows = toRows df
        let getGroupKey :: Row -> TypeLevelRow (SymbolsToSchema groupCols cols)
            getGroupKey = toTypeLevelRow @(SymbolsToSchema groupCols cols)

        return $! foldl' (\currentMap row ->
            let
                groupKey = getGroupKey row
                singleRowDf = DataFrame $ Map.map V.singleton row
            in
                Map.insertWith (\(DataFrame newMap) (DataFrame existingMap) ->
                    DataFrame $ Map.unionWith (V.++) existingMap newMap
                ) groupKey singleRowDf currentMap
            ) accMap rows
        ) (return Map.empty) return

-- | Groups a *sorted* `DataFrame` stream by a list of columns.
-- It yields a `DataFrame` for each distinct group.
-- The input stream *must* be sorted by the grouping columns for this to work correctly and efficiently.
