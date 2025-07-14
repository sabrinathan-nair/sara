{-# LANGUAGE PolyKinds #-}
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
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.List (sortBy)
import Sara.DataFrame.Types
import Data.Maybe (fromMaybe)
import Sara.DataFrame.Predicate (Predicate, evaluate)
import GHC.TypeLits (Symbol, KnownSymbol, symbolVal, CmpSymbol, TypeError, ErrorMessage(..))
import Data.Proxy (Proxy(..))
import Data.Kind (Type)

class AllKnownSymbol (xs :: [Symbol]) where
    symbolsToTexts :: Proxy xs -> [T.Text]

instance AllKnownSymbol '[] where
    symbolsToTexts _ = []

instance (KnownSymbol x, AllKnownSymbol xs) => AllKnownSymbol (x ': xs) where
    symbolsToTexts _ = T.pack (symbolVal (Proxy @x)) : symbolsToTexts (Proxy @xs)

-- | Filters rows from a DataFrame based on a type-safe predicate.
filterRows :: forall cols. KnownColumns cols => Predicate cols -> DataFrame cols -> DataFrame cols
filterRows p (DataFrame dfMap) =
    let
        colNames = columnNames (Proxy @cols)
        numRows = if Map.null dfMap then 0 else V.length (snd . head . Map.toList $ dfMap)

        -- Function to construct a row from a given index
        getRow :: Int -> Row
        getRow idx = Map.fromList [ (colName, (dfMap Map.! colName) V.! idx) | colName <- colNames ]

        -- Identify indices of rows that satisfy the predicate
        keptIndices = V.fromList [ idx | idx <- [0 .. numRows - 1], fromMaybe False (evaluate p (getRow idx)) ]

        newDfMap = if V.null keptIndices
                   then Map.empty
                   else
                       Map.fromList [ (colName, V.map (\idx -> (dfMap Map.! colName) V.! idx) keptIndices)
                                    | colName <- colNames
                                    ]
    in
        DataFrame newDfMap

-- | Sorts a DataFrame based on a list of type-safe sort criteria.
sortDataFrame :: forall cols. KnownColumns cols => [SortCriterion cols] -> DataFrame cols -> DataFrame cols
sortDataFrame sortCriteria df =
    let
        rows = toRows df
        compareRows :: forall a. (Ord a, CanBeDFValue a) => Proxy a -> DFValue -> DFValue -> Ordering
        compareRows _ val1 val2 = compare (fromDFValue @a val1) (fromDFValue @a val2)

        compareRows' :: Row -> Row -> Ordering
        compareRows' r1 r2 = foldr (\(SortCriterion (proxy :: Proxy col) order) acc ->
            let
                colName = T.pack (symbolVal proxy)
                val1 = Map.findWithDefault NA colName r1
                val2 = Map.findWithDefault NA colName r2
                colTypeProxy = Proxy :: Proxy (TypeOf col cols)
            in
                case compareRows colTypeProxy val1 val2 of
                    EQ -> acc
                    res -> case order of
                             Ascending -> res
                             Descending -> invertOrdering res
            ) EQ sortCriteria

        invertOrdering :: Ordering -> Ordering
        invertOrdering LT = GT
        invertOrdering EQ = EQ
        invertOrdering GT = LT

        sortedRows = sortBy compareRows' rows
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
type family DropColumns (toDrop :: [Symbol]) (cols :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    DropColumns '[] cols = cols
    DropColumns (d ': ds) cols = DropColumns ds (RemoveBySymbol d cols)

type family RemoveBySymbol (s :: Symbol) (cols :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    RemoveBySymbol s '[] = '[]
    RemoveBySymbol s ('(s, t) ': xs) = RemoveBySymbol s xs
    RemoveBySymbol s (x ': xs) = x ': RemoveBySymbol s xs

dropColumns :: forall (colsToDrop :: [Symbol]) (cols :: [(Symbol, Type)]) (newCols :: [(Symbol, Type)]).
              (KnownColumns newCols, KnownColumns cols, newCols ~ DropColumns colsToDrop cols, AllKnownSymbol colsToDrop)
              => DataFrame cols -> DataFrame newCols
dropColumns (DataFrame dfMap) =
    let
        colsToDropNames = symbolsToTexts (Proxy @colsToDrop)
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
type family RenameColumn (old :: Symbol) (new :: Symbol) (cols :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    RenameColumn old new (c ': cs) = RenameColumn' (CmpSymbol old (Fst c)) old new c cs
    RenameColumn old new '[] = '[]

type family RenameColumn' (ord :: Ordering) (old :: Symbol) (new :: Symbol) (c :: (Symbol, Type)) (cs :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    RenameColumn' 'EQ old new c cs = '(new, Snd c) ': RenameColumn old new cs
    RenameColumn' 'LT old new c cs = c ': RenameColumn old new cs
    RenameColumn' 'GT old new c cs = c ': RenameColumn old new cs

renameColumn :: forall (oldName :: Symbol) (newName :: Symbol) (cols :: [(Symbol, Type)]) (newCols :: [(Symbol, Type)]).
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
filterByBoolColumn :: forall (boolCol :: Symbol) (cols :: [(Symbol, Type)]).
                     (HasColumn boolCol cols, KnownColumns cols, TypeOf boolCol cols ~ Bool)
                     => Proxy boolCol -> DataFrame cols -> DataFrame cols
filterByBoolColumn _ df =
    let
        boolColName = T.pack (symbolVal (Proxy @boolCol))
        rows = toRows df
        filteredRows = filter (\row -> fromDFValue @Bool (Map.findWithDefault NA boolColName row) == Just True) rows
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
selectColumns :: forall (selectedCols :: [Symbol]) (cols :: [(Symbol, Type)]).
                (HasColumns selectedCols cols, KnownColumns (SelectCols selectedCols cols), AllKnownSymbol selectedCols)
                => DataFrame cols -> DataFrame (SelectCols selectedCols cols)
selectColumns (DataFrame dfMap) =
    let
        selectedColNames = symbolsToTexts (Proxy @selectedCols)
        newDfMap = Map.filterWithKey (\k _ -> k `elem` selectedColNames) dfMap
    in
        DataFrame newDfMap

type family SelectCols (selected :: [Symbol]) (cols :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    SelectCols '[] _ = '[]
    SelectCols (s ': ss) cols = SelectCols' s ss cols (Find s cols)

type family SelectCols' (s :: Symbol) (ss :: [Symbol]) (cols :: [(Symbol, Type)]) (found :: Maybe (Symbol, Type)) :: [(Symbol, Type)] where
    SelectCols' s ss cols ('Just pair) = pair ': SelectCols ss cols
    SelectCols' s ss cols 'Nothing = TypeError (Text "Column '" :<>: Text s :<>: Text "' not found.")


type family Find (s :: Symbol) (cols :: [(Symbol, Type)]) :: Maybe (Symbol, Type) where
    Find s '[] = 'Nothing
    Find s ('(s, t) ': _) = 'Just '(s, t)
    Find s (_ ': xs) = Find s xs

type family Fst (t :: (k, v)) :: k where
    Fst '(a, b) = a

type family MapFst (xs :: [(Symbol, Type)]) :: [Symbol] where
    MapFst '[] = '[]
    MapFst (x ': xs) = Fst x ': MapFst xs

type family Snd (t :: (k, v)) :: v where
    Snd '(a, b) = b