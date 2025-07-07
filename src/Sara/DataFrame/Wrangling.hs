{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module Sara.DataFrame.Wrangling (
    filterRows,
    sortDataFrame,
    dropColumns,
    dropRows,
    renameColumn,
    dropNA,
    fillNA,
    filterByBoolColumn,
    selectColumns
) where

import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.List (sortBy)
import Data.Ord (comparing)
import Sara.DataFrame.Types
import Sara.DataFrame.Predicate (Predicate, evaluate)
import GHC.TypeLits (Symbol, KnownSymbol, symbolVal, CmpSymbol)
import Data.Proxy (Proxy(..))
import Data.Type.Bool (If)
import Sara.DataFrame.Types (DataFrame(..), Row, DFValue(..), KnownColumns(..), SortOrder(..), SortCriterion(..), HasColumn, HasColumns, columnNames)

-- | Filters rows from a DataFrame based on a type-safe predicate.
filterRows :: forall cols. KnownColumns cols => Predicate cols -> DataFrame cols -> DataFrame cols
filterRows p df@(DataFrame dfMap) =
    let
        rows = toRows df
        filteredRows = filter (evaluate p) rows
        newDfMap = if null filteredRows
                   then Map.empty
                   else
                       let
                           colNames = columnNames (Proxy @cols)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- filteredRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap

-- | Sorts a DataFrame based on a list of type-safe sort criteria.
sortDataFrame :: forall cols. KnownColumns cols => [SortCriterion cols] -> DataFrame cols -> DataFrame cols
sortDataFrame sortCriteria df =
    let
        rows = toRows df
        compareRows :: Row -> Row -> Ordering
        compareRows r1 r2 = foldr (\(SortCriterion proxy order) acc ->
            let
                colName = T.pack (symbolVal proxy)
                val1 = Map.findWithDefault NA colName r1
                val2 = Map.findWithDefault NA colName r2
            in
                case compare val1 val2 of
                    EQ -> acc
                    res -> case order of
                             Ascending -> res
                             Descending -> invertOrdering res
            ) EQ sortCriteria

        invertOrdering :: Ordering -> Ordering
        invertOrdering LT = GT
        invertOrdering EQ = EQ
        invertOrdering GT = LT

        sortedRows = sortBy compareRows rows
        newDfMap = if null sortedRows
                   then Map.empty
                   else
                       let
                           colNames = columnNames (Proxy @cols)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- sortedRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap

-- | Drops a list of columns from a DataFrame.
type family DropColumns (toDrop :: [Symbol]) (cols :: [Symbol]) :: [Symbol] where
    DropColumns '[] cols = cols
    DropColumns (d ': ds) cols = DropColumns ds (Remove d cols)

dropColumns :: forall (colsToDrop :: [Symbol]) (cols :: [Symbol]) (newCols :: [Symbol]).
              (KnownColumns colsToDrop, KnownColumns cols, newCols ~ DropColumns colsToDrop cols, KnownColumns newCols)
              => DataFrame cols -> DataFrame newCols
dropColumns (DataFrame dfMap) =
    let
        colsToDropNames = columnNames (Proxy @colsToDrop)
        newDfMap = foldr Map.delete dfMap colsToDropNames
    in
        DataFrame newDfMap

-- | Drops rows from a DataFrame based on a list of row indices.
dropRows :: KnownColumns cols => [Int] -> DataFrame cols -> DataFrame cols
dropRows indicesToDrop (DataFrame dfMap) =
    let
        numRows = if Map.null dfMap then 0 else V.length (snd . head . Map.toList $ dfMap)
        indicesToKeep = V.fromList [ i | i <- [0 .. numRows - 1], i `notElem` indicesToDrop ]
        newDfMap = Map.map (\col -> V.map (col V.!) indicesToKeep) dfMap
    in
        DataFrame newDfMap

-- | Renames a column in a DataFrame.
type family RenameColumn (old :: Symbol) (new :: Symbol) (cols :: [Symbol]) :: [Symbol] where
    RenameColumn old new (c ': cs) = RenameColumn' (CmpSymbol old c) old new c cs
    RenameColumn old new '[] = '[]

type family RenameColumn' (ord :: Ordering) (old :: Symbol) (new :: Symbol) (c :: Symbol) (cs :: [Symbol]) :: [Symbol] where
    RenameColumn' 'EQ old new c cs = new ': RenameColumn old new cs
    RenameColumn' 'LT old new c cs = c ': RenameColumn old new cs
    RenameColumn' 'GT old new c cs = c ': RenameColumn old new cs

renameColumn :: forall (oldName :: Symbol) (newName :: Symbol) (cols :: [Symbol]) (newCols :: [Symbol]).
               (HasColumn oldName cols, KnownSymbol newName, newCols ~ RenameColumn oldName newName cols, KnownColumns newCols)
               => DataFrame cols -> DataFrame newCols
renameColumn (DataFrame dfMap) =
    let
        oldNameText = T.pack (symbolVal (Proxy @oldName))
        newNameText = T.pack (symbolVal (Proxy @newName))
        newDfMap = case Map.lookup oldNameText dfMap of
            Just col -> Map.insert newNameText col (Map.delete oldNameText dfMap)
            Nothing -> dfMap
    in
        DataFrame newDfMap

-- | Drops rows that contain any NA values.
dropNA :: forall cols. KnownColumns cols => DataFrame cols -> DataFrame cols
dropNA df =
    let
        rows = toRows df
        filteredRows = filter (not . any isNA . Map.elems) rows
        isNA NA = True
        isNA _ = False
        newDfMap = if null filteredRows
                   then Map.empty
                   else
                       let
                           colNames = columnNames (Proxy @cols)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- filteredRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap

-- | Replaces NA values with a specified DFValue.
fillNA :: KnownColumns cols => DFValue -> DataFrame cols -> DataFrame cols
fillNA replacementValue (DataFrame dfMap) =
    let
        transformColumn :: Column -> Column
        transformColumn = V.map (\val -> if val == NA then replacementValue else val)
        newDfMap = Map.map transformColumn dfMap
    in
        DataFrame newDfMap

-- | Filters a DataFrame based on a boolean column.
filterByBoolColumn :: forall (boolCol :: Symbol) (cols :: [Symbol]).
                     (HasColumn boolCol cols, KnownColumns cols)
                     => Proxy boolCol -> DataFrame cols -> DataFrame cols
filterByBoolColumn _ df = 
    let
        boolColName = T.pack (symbolVal (Proxy @boolCol))
        rows = toRows df
        filteredRows = filter (\row ->
            case Map.lookup boolColName row of
                Just (BoolValue True) -> True
                _ -> False
            ) rows
        newDfMap = if null filteredRows
                   then Map.empty
                   else
                       let
                           colNames = columnNames (Proxy @cols)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- filteredRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap

-- | Selects a subset of columns from a DataFrame.
selectColumns :: forall (selectedCols :: [Symbol]) (cols :: [Symbol]).
                (HasColumns selectedCols cols, KnownColumns selectedCols)
                => DataFrame cols -> DataFrame selectedCols
selectColumns df@(DataFrame dfMap) =
    let
        selectedColNames = columnNames (Proxy @selectedCols)
        newDfMap = Map.filterWithKey (\k _ -> k `elem` selectedColNames) dfMap
    in
        DataFrame newDfMap