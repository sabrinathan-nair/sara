{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | This module provides functions for transforming `DataFrame`s in a type-safe manner.
-- These transformations include selecting, adding, and modifying columns, as well as
-- reshaping the `DataFrame` itself.
module Sara.DataFrame.Transform (
    -- * Column Operations
    selectColumns,
    addColumn,
    applyColumn,
    mutate,
    AddColumn,
    -- * Row Operations
    filterRows,
    -- * Reshaping
    melt,
) where

import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.Types (DFValue(..), DataFrame(..), Row, toRows, fromRows, KnownColumns(..), CanBeDFValue(..), getDataFrameMap, TypeOf, HasColumn, toDFValue, type Append, type Nub, type UpdateColumn, type Fst, type Snd)
import Sara.Error (SaraError(..))
import Sara.DataFrame.Expression (Expr(..), evaluateExpr)
import Sara.DataFrame.Predicate (FilterPredicate, evaluate)
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import Data.Kind (Type)
import Data.Maybe (fromMaybe, fromJust)

import Streaming (Stream, Of)
import qualified Streaming.Prelude as S



import Data.Either (fromRight)

-- | Selects a subset of columns from a `DataFrame`.
-- The selected columns are specified by a type-level list of `(Symbol, Type)` tuples.
--
-- >>> :set -XDataKinds
-- >>> let df = fromRows @'["name" ::: T.Text, "age" ::: Int, "city" ::: T.Text] [Map.fromList [("name", TextValue "Alice"), ("age", IntValue 25), ("city", TextValue "New York")]
-- >>> let selectedDf = selectColumns @'["name" ::: T.Text, "age" ::: Int] df
-- >>> columnNames (Proxy @(Schema selectedDf))
-- ["name","age"]
selectColumns :: forall (selectedCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)]).
                (KnownColumns selectedCols, KnownColumns originalCols)
                => DataFrame originalCols -> DataFrame selectedCols
selectColumns (DataFrame dfMap) =
    let
        selectedColNames = columnNames (Proxy @selectedCols)
        selectedMap = Map.filterWithKey (\k _ -> k `elem` selectedColNames) dfMap
    in
        DataFrame selectedMap

-- | A type family that adds a new column to a schema.
-- If the column already exists, it is replaced.
type family AddColumn (newCol :: (Symbol, Type)) (cols :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    AddColumn newCol cols = Nub (newCol ': cols)

-- | Adds a new column to a `DataFrame` or modifies an existing one.
-- The new column is defined by a type-safe expression.
addColumn :: forall (newCol :: (Symbol, Type)) (cols :: [(Symbol, Type)]) (newCols :: [(Symbol, Type)]).
            (KnownSymbol (Fst newCol), KnownColumns cols, newCols ~ AddColumn newCol cols, KnownColumns newCols, CanBeDFValue (Snd newCol))
            => Expr cols (Snd newCol) -> DataFrame cols -> Either SaraError (DataFrame newCols)
addColumn expr (DataFrame dfMap) =
    let
        colName = T.pack (symbolVal (Proxy :: Proxy (Fst newCol)))
        numRows = case Map.toList dfMap of
            [] -> 0
            (_, col) : _ -> V.length col
        newColumnValues = V.fromList <$> traverse (\idx -> toDFValue <$> evaluateExpr expr dfMap idx) [0 .. numRows - 1]
    in
        case newColumnValues of
            Right vals -> Right $ DataFrame (Map.insert colName vals dfMap)
            Left err -> Left err


-- | A type family that calculates the schema of a `DataFrame` after a `melt` operation.
type family Melt (id_vars :: [(Symbol, Type)]) (value_vars :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    Melt id_vars value_vars = Nub (Append id_vars '[ '("variable", T.Text), '("value", DFValue)])

-- | Unpivots a `DataFrame` from a wide to a long format.
-- The `id_vars` are the columns to keep, and the `value_vars` are the columns to unpivot.
-- The resulting `DataFrame` will have two new columns: "variable" and "value".
melt :: forall (id_vars :: [(Symbol, Type)]) (value_vars :: [(Symbol, Type)]) (cols :: [(Symbol, Type)]) (newCols :: [(Symbol, Type)]).
       (KnownColumns id_vars, KnownColumns value_vars, KnownColumns cols, newCols ~ Melt id_vars value_vars, KnownColumns newCols)
       => DataFrame cols -> DataFrame newCols
melt df =
    let
        idVarNames = columnNames (Proxy :: Proxy id_vars)
        valueVarNames = columnNames (Proxy :: Proxy value_vars)
        rows = toRows df
        processRow :: Row -> [Row]
        processRow row =
            let
                idValues = Map.filterWithKey (\k _ -> k `elem` idVarNames) row
            in
                concatMap (\valVar ->
                    case Map.lookup valVar row of
                        Just val -> [Map.union idValues (Map.fromList [(T.pack "variable", TextValue valVar), (T.pack "value", val)])]
                        Nothing -> []
                ) valueVarNames

        meltedRows = concatMap processRow rows
        newDfMap = if null meltedRows
                   then Map.empty
                   else
                       let
                           colNames = columnNames (Proxy :: Proxy newCols)
                           cols' = [ V.fromList [ Map.findWithDefault NA colName r | r <- meltedRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols')
    in
        DataFrame newDfMap



-- | Applies a function to a specified column in a `DataFrame` in a type-safe way.
-- The function takes the old column type and returns the new column type.
-- The schema of the `DataFrame` is updated accordingly.
applyColumn :: forall col oldType newType cols newCols.
              (HasColumn col cols, KnownColumns cols, CanBeDFValue oldType, CanBeDFValue newType, TypeOf col cols ~ oldType, newCols ~ UpdateColumn col newType cols, KnownColumns newCols)
              => Proxy col -> (oldType -> newType) -> Stream (Of (DataFrame cols)) IO () -> Stream (Of (Either SaraError (DataFrame newCols))) IO ()
applyColumn colProxy f =
    S.mapM (\df -> do
        let colName = T.pack (symbolVal colProxy)
        let dfMap = getDataFrameMap df
        let oldColumn = dfMap Map.! colName
        let newColumn = V.map (\val -> fromRight NA (toDFValue . f <$> fromDFValue val)) oldColumn
        let newDfMap = Map.insert colName newColumn dfMap
        return $ Right $ DataFrame newDfMap
    )


-- | Adds a new column or modifies an existing one based on a type-safe expression.
-- This is a synonym for `addColumn`.
mutate :: forall newColName newColType cols newCols.
         (KnownSymbol newColName, CanBeDFValue newColType, KnownColumns cols, newCols ~ AddColumn '(newColName, newColType) cols, KnownColumns newCols)
         => Proxy newColName
         -> Expr cols newColType
         -> DataFrame cols
         -> Either SaraError (DataFrame newCols)
mutate newColProxy expr df = addColumn @'(newColName, newColType) expr df

-- | Filters rows from a DataFrame based on a type-safe predicate.
filterRows :: forall cols.
             KnownColumns cols
             => FilterPredicate cols
             -> Stream (Of (DataFrame cols)) IO ()
             -> Stream (Of (Either SaraError (DataFrame cols))) IO ()
filterRows predicate = S.mapM (\df -> do
    let dfMap = getDataFrameMap df
    let rows = toRows df
    let filteredRows = [row | (row, idx) <- zip rows [0..], fromRight False (evaluate predicate dfMap idx)]
    if null filteredRows
        then return $ Right $ DataFrame Map.empty
        else return $ Right $ fromRows filteredRows)
