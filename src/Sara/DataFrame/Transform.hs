{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
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
    -- * Reshaping
    melt,
) where

import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.Types
import Sara.DataFrame.Expression (Expr(..), evaluateExpr)
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import Data.Kind (Type)

import Sara.DataFrame.Apply

-- | A type family to extract the first element of a tuple.
type family Fst (t :: (k, v)) :: k where
    Fst '(a, b) = a

-- | A type family to extract the second element of a tuple.
type family Snd (t :: (k, v)) :: v where
    Snd '(a, b) = b

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
            => Expr cols (Snd newCol) -> DataFrame cols -> DataFrame newCols
addColumn expr (DataFrame dfMap) =
    let
        colName = T.pack (symbolVal (Proxy :: Proxy (Fst newCol)))
        numRows = if Map.null dfMap then 0 else V.length (snd . head . Map.toList $ dfMap)
        newColumnValues = V.fromList [ toDFValue (evaluateExpr expr dfMap idx) | idx <- [0 .. numRows - 1] ]
        updatedDfMap = Map.insert colName newColumnValues dfMap
    in
        DataFrame updatedDfMap


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
              => Proxy col -> (oldType -> newType) -> DataFrame cols -> DataFrame newCols
applyColumn colProxy f = apply (Apply colProxy f)


-- | Adds a new column or modifies an existing one based on a type-safe expression.
-- This is a synonym for `addColumn`.
mutate :: forall newColName newColType cols newCols.
         (KnownSymbol newColName, CanBeDFValue newColType, KnownColumns cols, newCols ~ AddColumn '(newColName, newColType) cols, KnownColumns newCols)
         => Proxy newColName
         -> Expr cols newColType
         -> DataFrame cols
         -> DataFrame newCols
mutate newColProxy expr (DataFrame dfMap) =
    let
        newColName = T.pack (symbolVal newColProxy)
        numRows = if Map.null dfMap then 0 else V.length (snd . head . Map.toList $ dfMap)
        newColumnValues = V.fromList [ toDFValue (evaluateExpr expr dfMap idx) | idx <- [0 .. numRows - 1] ]
        updatedDfMap = Map.insert newColName newColumnValues dfMap
    in
        DataFrame updatedDfMap
