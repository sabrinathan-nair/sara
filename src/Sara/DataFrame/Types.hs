{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Sara.DataFrame.Types (
    DFValue(..),
    Column,
    DataFrame(..),
    getDataFrameMap,
    Row,
    toRows,
    fromRows,
    SortOrder(..),
    SortCriterion(..),
    SortableColumn,
    KnownColumns(..),
    KnownSymbols(..),
    CanBeDFValue(..),
    -- * Type-level programming helpers
    type Append,
    type Remove,
    type Nub,
    HasColumn,
    HasColumns,
    TypeOf,
    GetColumnTypes,
    GetColumnNames,
    SymbolsToSchema,
    UpdateColumn,
    TypeLevelRow(..),
    toTypeLevelRow,
    fromTypeLevelRow,
    isNA,
    MapTypes,
    All,
    ContainsColumn,
    ConcatAxis(..),
    JoinType(..),
    type JoinCols,
    fromDFValueUnsafe,
    type (:::),
    type Fst,
    type Snd
) where

import qualified Data.Text as T
import Data.Time (Day, UTCTime)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import Data.Time.Format (parseTimeM, defaultTimeLocale, formatTime)
import Text.Read (readMaybe)
import Data.Proxy (Proxy(..))
import GHC.TypeLits (symbolVal, Symbol, ErrorMessage(Text, (:<>:), ShowType), KnownSymbol, TypeError, CmpSymbol)
import Data.Kind (Type, Constraint)
import Data.Typeable (TypeRep, Typeable, typeRep)
import Data.Maybe (fromMaybe)
import qualified Data.Vector as V
import Data.Scientific (toRealFloat)
import GHC.Generics (Generic)
import Control.DeepSeq (NFData)
import Data.Aeson (ToJSON, FromJSON, toJSON, parseJSON, Value(Number, String, Bool, Null))


type family Fst (pair :: (k1, k2)) :: k1 where
    Fst '(x, _) = x

type family Snd (pair :: (k1, k2)) :: k2 where
    Snd '(_, y) = y

infixr 5 :::
type family (a :: k) ::: (b :: [k]) :: [k] where
    a ::: '[] = '[a]
    a ::: (x ': xs) = a ': x ': xs
    
    



isNA :: DFValue -> Bool
isNA NA = True
isNA _ = False

-- | A type to represent a single value in a DataFrame.
-- It can hold different types of data such as integers, doubles, text, dates, booleans, or missing values (NA).
data DFValue = IntValue Int
           | DoubleValue Double
           | TextValue T.Text
           | DateValue Day
           | TimestampValue UTCTime
           | BoolValue Bool
           | NA -- ^ Represents a missing value.
           deriving (Show, Eq, Ord, Generic, NFData)

-- | ToJSON instance for DFValue, allowing conversion to JSON.
instance ToJSON DFValue where
    toJSON (IntValue i) = toJSON i
    toJSON (DoubleValue d) = toJSON d
    toJSON (TextValue t) = toJSON t
    toJSON (DateValue d) = toJSON (formatTime defaultTimeLocale "%Y-%m-%d" d)
    toJSON (TimestampValue t) = toJSON t
    toJSON (BoolValue b) = toJSON b
    toJSON NA = Null

-- | FromJSON instance for DFValue, allowing parsing from JSON.
instance FromJSON DFValue where
    parseJSON (Number n) = pure (DoubleValue (toRealFloat n))
    parseJSON (String s) =
        -- Try parsing as Date, then Bool, then Int, then Double, otherwise Text
        case parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" (T.unpack s) :: Maybe UTCTime of
            Just t -> pure (TimestampValue t)
            Nothing ->
                case parseTimeM True defaultTimeLocale "%Y-%m-%d" (T.unpack s) :: Maybe Day of
                    Just d -> pure (DateValue d)
                    Nothing -> case readMaybe (T.unpack s) :: Maybe Int of
                        Just i -> pure (IntValue i)
                        Nothing -> case readMaybe (T.unpack s) :: Maybe Double of
                            Just d -> pure (DoubleValue d)
                            Nothing -> case T.toLower s of
                                "true" -> pure (BoolValue True)
                                "false" -> pure (BoolValue False)
                                _ -> pure (TextValue s)
    parseJSON (Bool b) = pure (BoolValue b)
    parseJSON Null = pure NA
    parseJSON _ = fail "Unsupported JSON value type for DFValue"

-- | A column in a DataFrame, represented as a 'Vector' of 'DFValue's.
-- Using 'Vector' provides efficient storage and operations.
type Column = Vector DFValue

-- | A row in a DataFrame, represented as a 'Map' from column names ('T.Text') to 'DFValue's.
type Row = Map T.Text DFValue

-- | The DataFrame itself, represented as a newtype wrapper around a 'Map' from column names ('T.Text') to 'Column's.
-- This structure allows for efficient column-wise operations and access.
newtype DataFrame (cols :: [(Symbol, Type)]) = DataFrame (Map T.Text Column)
    deriving (Generic)

getDataFrameMap :: DataFrame cols -> Map T.Text Column
getDataFrameMap (DataFrame dfMap) = dfMap

instance NFData (DataFrame cols)

-- | A type to represent a single row with its schema at the type level.
newtype TypeLevelRow (cols :: [(Symbol, Type)]) = TypeLevelRow (Map T.Text DFValue)
    deriving (Show, Eq, Ord) -- Placeholder for now, might need custom instances



-- | Converts a runtime Row to a type-level Row, ensuring schema conformity.
toTypeLevelRow :: forall cols. KnownColumns cols => Row -> TypeLevelRow cols
toTypeLevelRow row =
    let expectedNames = columnNames (Proxy @cols)
        filteredRow = Map.filterWithKey (\k _ -> k `elem` expectedNames) row
    in TypeLevelRow filteredRow

-- | Converts a type-level Row back to a runtime Row.
fromTypeLevelRow :: TypeLevelRow cols -> Row
fromTypeLevelRow (TypeLevelRow row) = row

-- | Type class to get the runtime Text names from a type-level list of Symbols.
class KnownColumns (cols :: [(Symbol, Type)]) where
    columnNames :: Proxy cols -> [T.Text]
    columnTypes :: Proxy cols -> [TypeRep]

class KnownSymbols (ss :: [Symbol]) where
  symbolVals :: Proxy ss -> [String]

instance KnownSymbols '[] where
  symbolVals _ = []

instance (KnownSymbol s, KnownSymbols ss) => KnownSymbols (s ': ss) where
  symbolVals _ = symbolVal (Proxy @s) : symbolVals (Proxy @ss)

instance KnownColumns '[] where
    columnNames _ = []
    columnTypes _ = []

instance (KnownSymbol x, Typeable a, KnownColumns xs) => KnownColumns ('(x, a) ': xs) where
    columnNames _ = T.pack (symbolVal (Proxy @x)) : columnNames (Proxy @xs)
    columnTypes _ = typeRep (Proxy @a) : columnTypes (Proxy @xs)



instance (KnownColumns cols) => Show (DataFrame cols) where
    show (DataFrame dfMap) =
        let
            cols = columnNames (Proxy @cols)
            header = T.intercalate "\t" cols
            rows = toRows (DataFrame dfMap)
            rowStrings = map (\row -> T.intercalate "\t" [ maybe "NA" (T.pack . show) (Map.lookup col row) | col <- cols ]) rows
        in
            T.unpack $ T.intercalate "\n" (header : rowStrings)

instance Eq (DataFrame cols) where
    (DataFrame dfMap1) == (DataFrame dfMap2) = dfMap1 == dfMap2

-- | Specifies the sort order for a column.
data SortOrder = Ascending  -- ^ Sort in ascending order.
               | Descending -- ^ Sort in descending order.
    deriving (Show, Eq)

-- | A type-safe criterion for sorting a DataFrame.
data SortCriterion (cols :: [(Symbol, Type)]) where
    SortCriterion :: (KnownSymbol col, HasColumn col cols, Ord (TypeOf col cols), CanBeDFValue (TypeOf col cols)) => Proxy col -> SortOrder -> SortCriterion cols

-- | A type synonym for a sortable column.
type SortableColumn (col :: Symbol) (cols :: [(Symbol, Type)]) = (KnownSymbol col, HasColumn col cols)

-- | Specifies the axis along which to concatenate DataFrames.
data ConcatAxis = ConcatRows    -- ^ Concatenate DataFrames row-wise.
                | ConcatColumns -- ^ Concatenate DataFrames column-wise.
    deriving (Show, Eq)

-- | Specifies the type of join to be performed.
data JoinType = InnerJoin | LeftJoin | RightJoin | OuterJoin
    deriving (Show, Eq)

-- | A type class for values that can be converted to and from DFValue.
class (Typeable a) => CanBeDFValue a where
    toDFValue :: a -> DFValue
    fromDFValue :: DFValue -> Maybe a

instance CanBeDFValue Int where
    toDFValue = IntValue
    fromDFValue (IntValue i) = Just i
    fromDFValue _ = Nothing

instance CanBeDFValue Double where
    toDFValue = DoubleValue
    fromDFValue (DoubleValue d) = Just d
    fromDFValue _ = Nothing

instance CanBeDFValue T.Text where
    toDFValue = TextValue
    fromDFValue (TextValue t) = Just t
    fromDFValue _ = Nothing

instance CanBeDFValue Day where
    toDFValue = DateValue
    fromDFValue (DateValue d) = Just d
    fromDFValue _ = Nothing

instance CanBeDFValue UTCTime where
    toDFValue = TimestampValue
    fromDFValue (TimestampValue t) = Just t
    fromDFValue _ = Nothing

instance CanBeDFValue Bool where
    toDFValue = BoolValue
    fromDFValue (BoolValue b) = Just b
    fromDFValue _ = Nothing

instance CanBeDFValue a => CanBeDFValue (Maybe a) where
    toDFValue Nothing = NA
    toDFValue (Just a) = toDFValue a
    fromDFValue NA = Just Nothing
    fromDFValue x = Just <$> fromDFValue x

fromDFValueUnsafe :: CanBeDFValue a => DFValue -> a
fromDFValueUnsafe dfValue = fromMaybe (error "fromDFValueUnsafe: NA or type mismatch") (fromDFValue dfValue)

-- | Converts a 'DataFrame' into a list of 'Row's.
-- Each 'Row' is a 'Map' where keys are column names and values are the corresponding 'DFValue's for that row.
toRows :: DataFrame cols -> [Row]
toRows (DataFrame dfMap) =
    if Map.null dfMap
        then []
        else
            let
                -- Assuming all columns have the same number of rows
                numRows = V.length (snd . head . Map.toList $ dfMap)
                columnNames' = Map.keys dfMap
            in
                [ Map.fromList [ (colName, (dfMap Map.! colName) V.! rowIndex) | colName <- columnNames' ]
                | rowIndex <- [0 .. numRows - 1]
                ]

-- * Type-level list operations

-- | Create a DataFrame from a list of rows.
fromRows :: KnownColumns cols => [Row] -> DataFrame cols
fromRows [] = DataFrame Map.empty
fromRows rows@(firstRow:_) =
    let columns = Map.keys firstRow
        colMap = Map.fromList $ map (\colName -> (colName, V.fromList $ map (Map.! colName) rows)) columns
    in DataFrame colMap

-- | A type family to get the type of a column from a schema.
type family TypeOf (s :: Symbol) (cols :: [(Symbol, Type)]) :: Type where
  TypeOf s ('(s, t) ': xs) = t
  TypeOf s (_ ': xs) = TypeOf s xs
  TypeOf s '[] = TypeError (Text "Column '" :<>: ShowType s :<>: Text "' not found in DataFrame schema.")

-- | A type family to append two type-level lists.
type family Append (xs :: [k]) (ys :: [k]) :: [k] where
    -- | Appends two type-level lists. Used for combining schemas.
    Append '[] ys = ys
    Append (x ': xs) ys = x ': Append xs ys

-- | A type family to remove an element from a type-level list.
type family Remove (x :: k) (ys :: [k]) :: [k] where
    -- | Removes an element from a type-level list. Used for schema manipulation.
    Remove x '[] = '[]
    Remove x (x ': ys) = Remove x ys
    Remove x (y ': ys) = y ': Remove x ys

-- | A type family to remove duplicates from a type-level list.
type family Nub (xs :: [k]) :: [k] where
    -- | Removes duplicates from a type-level list. Used for schema manipulation.
    Nub '[] = '[]
    Nub (x ': xs) = x ': Nub (Remove x xs)

-- | A constraint to check if a column is present in a list of columns, with a custom type error.
type family CheckHasColumn (s :: Symbol) (ss :: [(Symbol, Type)]) :: Constraint where
  -- | Checks if a column is present in a list of columns, with a custom type error.
  CheckHasColumn s '[] = TypeError (Text "Column '" :<>: ShowType s :<>: Text "' not found in DataFrame schema.")
  CheckHasColumn s ('(h, t) ': rest) = CheckHasColumnImpl (CmpSymbol s h) s rest

type family CheckHasColumnImpl (o :: Ordering) (s :: Symbol) (rest :: [(Symbol, Type)]) :: Constraint where
  CheckHasColumnImpl 'EQ s rest = ()
  CheckHasColumnImpl _ s rest = CheckHasColumn s rest

-- | A constraint synonym for checking if a column exists in a DataFrame.
type HasColumn (col :: Symbol) (cols :: [(Symbol, Type)]) = (KnownSymbol col, CheckHasColumn col cols)

-- | Constraint to ensure a list of columns exists in another list of columns.
type family HasColumns (subset :: [Symbol]) (superset :: [(Symbol, Type)]) :: Constraint where
    -- | Constraint to ensure a list of columns exists in another list of columns.
    HasColumns '[] _ = ()
    HasColumns (s ': ss) superset = (HasColumn s superset, HasColumns ss superset)







type family GetColumnNames (cols :: [(Symbol, Type)]) :: [Symbol] where
    GetColumnNames '[] = '[]
    GetColumnNames ('(s, t) ': xs) = s ': GetColumnNames xs

type family GetColumnTypes (cols :: [(Symbol, Type)]) :: [Type] where
    GetColumnTypes '[] = '[]
    GetColumnTypes ('(s, t) ': xs) = t ': GetColumnTypes xs

type family SymbolsToSchema (syms :: [Symbol]) (originalSchema :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    SymbolsToSchema '[] _ = '[]
    SymbolsToSchema (s ': ss) originalSchema = '(s, TypeOf s originalSchema) ': SymbolsToSchema ss originalSchema




type family UpdateColumn (col :: Symbol) (newType :: Type) (cols :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    UpdateColumn col newType '[] = '[]
    UpdateColumn col newType ('(col, oldType) ': xs) = '(col, newType) ': xs
    UpdateColumn col newType (x ': xs) = x ': UpdateColumn col newType xs


type family MapTypes (ts :: [Type]) :: Constraint where
    MapTypes '[] = ()
    MapTypes (t ': ts) = (CanBeDFValue t, MapTypes ts)

type family All (c :: k -> Constraint) (xs :: [k]) :: Constraint where
    All c '[] = ()
    All c (x ': xs) = (c x, All c xs)

type family ContainsColumn (s :: Symbol) (cols :: [(Symbol, Type)]) :: Bool where
    ContainsColumn s '[] = 'False
    ContainsColumn s ('(s, _) ': _) = 'True
    ContainsColumn s (_ ': xs) = ContainsColumn s xs

type family JoinCols (cols1 :: [(Symbol, Type)]) (cols2 :: [(Symbol, Type)]) :: [(Symbol, Type)] where
    JoinCols cols1 cols2 = Nub (Append cols1 cols2)










