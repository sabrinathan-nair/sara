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

module Sara.DataFrame.Transform (
    selectColumns,
    addColumn,
    melt,
    applyColumn,
    mutate
) where

import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.Types
import Sara.DataFrame.Expression (Expr, evaluateExpr)
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import Data.Maybe (fromMaybe)
import Data.Kind (Type)
import Data.Typeable (Typeable, typeRep)

type family Fst (t :: (k, v)) :: k where
    Fst '(a, b) = a

-- | Selects a subset of columns from a DataFrame.
selectColumns :: forall (selectedCols :: [(Symbol, Type)]) (originalCols :: [(Symbol, Type)]).
                (KnownColumns selectedCols, KnownColumns originalCols)
                => DataFrame originalCols -> DataFrame selectedCols
selectColumns (DataFrame dfMap) =
    let
        selectedColNames = columnNames (Proxy @selectedCols)
        selectedMap = Map.filterWithKey (\k _ -> k `elem` selectedColNames) dfMap
    in
        DataFrame selectedMap

-- | Adds a new column to a DataFrame or modifies an existing one.
type family AddColumn (newCol :: (Symbol, Type)) (cols :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    AddColumn newCol cols = Nub (newCol ': cols)

addColumn :: forall (newCol :: (Symbol, Type)) (cols :: [(Symbol, Type)]) (newCols :: [(Symbol, Type)]).
            (KnownSymbol (Fst newCol), KnownColumns cols, newCols ~ AddColumn newCol cols, KnownColumns newCols)
            => (Row -> DFValue) -> DataFrame cols -> DataFrame newCols
addColumn f df@(DataFrame dfMap) =
    let
        colName = T.pack (symbolVal (Proxy :: Proxy (Fst newCol)))
        rows = toRows df
        newColumnValues = V.fromList $ map f rows
        updatedDfMap = Map.insert colName newColumnValues dfMap
    in
        DataFrame updatedDfMap

-- | Unpivots a DataFrame from wide format to long format.
type family Melt (id_vars :: [(Symbol, Type)]) (value_vars :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    Melt id_vars value_vars = Nub (Append id_vars '[ '("variable", T.Text), '("value", DFValue)])

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

-- | Applies a function to a specified column in a DataFrame in a type-safe way.
applyColumn :: forall col oldType newType cols newCols.
              (HasColumn col cols, KnownColumns cols, CanBeDFValue oldType, CanBeDFValue newType, TypeOf col cols ~ oldType, newCols ~ UpdateColumn col newType cols, KnownColumns newCols)
              => Proxy col -> (oldType -> newType) -> DataFrame cols -> DataFrame newCols
applyColumn _ f (DataFrame dfMap) =
    let
        colName = T.pack (symbolVal (Proxy :: Proxy col))
        transform v = case fromDFValue v of
                        Just x -> toDFValue (f x)
                        Nothing -> NA -- If type mismatch, replace with NA
    in case Map.lookup colName dfMap of
        Just c ->
            let updatedCol = V.map transform c
            in DataFrame (Map.insert colName updatedCol dfMap)
        Nothing ->
            DataFrame dfMap

-- | Adds a new column or modifies an existing one based on a type-safe expression.
mutate :: forall newColName newColType cols newCols.
         (KnownSymbol newColName, CanBeDFValue newColType, KnownColumns cols, newCols ~ AddColumn '(newColName, newColType) cols, KnownColumns newCols)
         => Proxy newColName
         -> Expr cols newColType
         -> DataFrame cols
         -> DataFrame newCols
mutate newColProxy expr df@(DataFrame dfMap) =
    let
        rows = toRows df
        newColName = T.pack (symbolVal newColProxy)
        newColumnValues = V.fromList $ map (\row -> fromMaybe NA (toDFValue <$> evaluateExpr expr row)) rows
        updatedDfMap = Map.insert newColName newColumnValues dfMap
    in
        DataFrame updatedDfMap
