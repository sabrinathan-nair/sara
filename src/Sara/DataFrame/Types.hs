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
    type (:++:),
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
    type Snd,
    prop_fromRows_toRows_identity,
    getDFValueType
) where

import qualified Data.Text as T
import Data.Time (Day, UTCTime)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import Data.Time.Format (parseTimeM, defaultTimeLocale, formatTime)
import Text.Read (readEither)
import Data.Proxy (Proxy(..))
import GHC.TypeLits (symbolVal, Symbol, ErrorMessage(Text, (:<>:), ShowType), KnownSymbol, TypeError, CmpSymbol)
import Data.Kind (Type, Constraint)
import Data.Typeable (TypeRep, Typeable, typeRep)

import Sara.Error (SaraError(..))
import Control.Monad.Fail (fail)
import qualified Data.Vector as V
import Data.Scientific (toRealFloat)
import GHC.Generics (Generic)
import Control.DeepSeq (NFData)
import Data.Aeson as A
import Test.QuickCheck
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime(..), secondsToDiffTime)


-- | A type class for generating arbitrary DFValues of a specific type.
class ArbitraryDFValueType a where
    arbitraryDFValue :: Gen DFValue

instance ArbitraryDFValueType Int where
    arbitraryDFValue = IntValue <$> arbitrary

instance ArbitraryDFValueType Double where
    arbitraryDFValue = DoubleValue <$> arbitrary

instance ArbitraryDFValueType T.Text where
    arbitraryDFValue = TextValue . T.pack <$> listOf (elements ['a'..'z'])

instance ArbitraryDFValueType Day where
    arbitraryDFValue = DateValue <$> arbitraryDay

instance ArbitraryDFValueType UTCTime where
    arbitraryDFValue = TimestampValue <$> arbitraryUTCTime

instance ArbitraryDFValueType Bool where
    arbitraryDFValue = BoolValue <$> arbitrary

-- Arbitrary instance for DFValue
instance Arbitrary DFValue where
    arbitrary = oneof [
        IntValue <$> arbitrary,
        DoubleValue <$> arbitrary,
        TextValue . T.pack <$> listOf (elements ['a'..'z']),
        DateValue <$> arbitraryDay,
        TimestampValue <$> arbitraryUTCTime,
        BoolValue <$> arbitrary,
        return NA
        ]

arbitraryDay :: Gen Day
arbitraryDay = fromGregorian <$> arbitrary <*> choose (1,12) <*> choose (1,28)

arbitraryUTCTime :: Gen UTCTime
arbitraryUTCTime = UTCTime <$> arbitraryDay <*> (secondsToDiffTime <$> choose (0, 86400))

-- Arbitrary instance for DataFrame
instance (KnownColumns cols, GencolRow cols) => Arbitrary (DataFrame cols) where
    arbitrary = do
        numRows <- choose (1, 10) -- Generate 1 to 10 rows
        rowsList <- vectorOf numRows (gencolRow (Proxy @cols))
        return $ fromRows (map Map.fromList rowsList)

class GencolRow (cols :: [(Symbol, Type)]) where
    gencolRow :: Proxy cols -> Gen [(T.Text, DFValue)]

instance GencolRow '[] where
    gencolRow _ = return []

instance (KnownSymbol s, Typeable t, ArbitraryDFValueType t, GencolRow rest) => GencolRow ('(s, t) : rest) where
    gencolRow _ = do
        let colName = T.pack (symbolVal (Proxy @s))
        val <- arbitraryDFValue @t
        restOfRow <- gencolRow (Proxy @rest)
        return $ (colName, val) : restOfRow


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

-- | Returns the TypeRep of the value contained within a DFValue, or Nothing if it's NA.
getDFValueType :: DFValue -> Maybe TypeRep
getDFValueType (IntValue _) = Just (typeRep (Proxy @Int))
getDFValueType (DoubleValue _) = Just (typeRep (Proxy @Double))
getDFValueType (TextValue _) = Just (typeRep (Proxy @T.Text))
getDFValueType (DateValue _) = Just (typeRep (Proxy @Day))
getDFValueType (TimestampValue _) = Just (typeRep (Proxy @UTCTime))
getDFValueType (BoolValue _) = Just (typeRep (Proxy @Bool))
getDFValueType NA = Nothing

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
    toJSON (IntValue i) = A.Number (fromIntegral i)
    toJSON (DoubleValue d) = toJSON d
    toJSON (TextValue t) = toJSON t
    toJSON (DateValue d) = toJSON (formatTime defaultTimeLocale "%Y-%m-%d" d)
    toJSON (TimestampValue t) = toJSON t
    toJSON (BoolValue b) = toJSON b
    toJSON NA = Null

-- | FromJSON instance for DFValue, allowing parsing from JSON.
instance FromJSON DFValue where
    parseJSON (A.Number n) = 
        case A.fromJSON (A.Number n) :: A.Result Int of
            A.Success i -> pure (IntValue i)
            A.Error _ -> pure (DoubleValue (toRealFloat n))
    parseJSON (A.String s) =
        -- Try parsing as Date, then Bool, then Int, then Double, otherwise Text
        case parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" (T.unpack s) :: Maybe UTCTime of
            Just t -> pure (TimestampValue t)
            Nothing ->
                case parseTimeM True defaultTimeLocale "%Y-%m-%d" (T.unpack s) :: Maybe Day of
                    Just d -> pure (DateValue d)
                    Nothing -> case readEither (T.unpack s) :: Either String Int of
                        Right i -> pure (IntValue i)
                        Left _ -> case readEither (T.unpack s) :: Either String Double of
                            Right d -> pure (DoubleValue d)
                            Left _ -> case T.toLower s of
                                "true" -> pure (BoolValue True)
                                "false" -> pure (BoolValue False)
                                _ -> pure (TextValue s)
    parseJSON (A.Bool b) = pure (BoolValue b)
    parseJSON A.Null = pure NA
    parseJSON _ = Control.Monad.Fail.fail "Unsupported JSON value type for DFValue"

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

instance Semigroup (DataFrame cols) where
    (DataFrame dfMap1) <> (DataFrame dfMap2) = DataFrame $ Map.unionWith (V.++) dfMap1 dfMap2

instance Monoid (DataFrame cols) where
    mempty = DataFrame Map.empty

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
    columnSchema :: Proxy cols -> [(T.Text, TypeRep)]

class KnownSymbols (ss :: [Symbol]) where
  symbolVals :: Proxy ss -> [String]

instance KnownSymbols '[] where
  symbolVals _ = []

instance (KnownSymbol s, KnownSymbols ss) => KnownSymbols (s ': ss) where
  symbolVals _ = symbolVal (Proxy @s) : symbolVals (Proxy @ss)

instance KnownColumns '[] where
    columnNames _ = []
    columnTypes _ = []
    columnSchema _ = []

instance (KnownSymbol x, Typeable a, KnownColumns xs) => KnownColumns ('(x, a) ': xs) where
    columnNames _ = T.pack (symbolVal (Proxy @x)) : columnNames (Proxy @xs)
    columnTypes _ = typeRep (Proxy @a) : columnTypes (Proxy @xs)
    columnSchema _ = (T.pack (symbolVal (Proxy @x)), typeRep (Proxy @a)) : columnSchema (Proxy @xs)



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

instance Arbitrary SortOrder where
    arbitrary = elements [Ascending, Descending]

-- | A type-safe criterion for sorting a DataFrame.
data SortCriterion (cols :: [(Symbol, Type)]) where
    SortCriterion :: (KnownSymbol col, HasColumn col cols, Ord (TypeOf col cols), CanBeDFValue (TypeOf col cols)) => Proxy col -> SortOrder -> SortCriterion cols

instance Show (SortCriterion cols) where
    show (SortCriterion proxy order) = "SortCriterion " ++ symbolVal proxy ++ " " ++ show order

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
    fromDFValue :: DFValue -> Either SaraError a

instance CanBeDFValue Int where
    toDFValue = IntValue
    fromDFValue (IntValue i) = Right i
    fromDFValue other = Left $ TypeMismatch "Int" (T.pack $ show other)

instance CanBeDFValue Double where
    toDFValue = DoubleValue
    fromDFValue (DoubleValue d) = Right d
    fromDFValue other = Left $ TypeMismatch "Double" (T.pack $ show other)

instance CanBeDFValue T.Text where
    toDFValue = TextValue
    fromDFValue (TextValue t) = Right t
    fromDFValue other = Left $ TypeMismatch "Text" (T.pack $ show other)

instance CanBeDFValue Day where
    toDFValue = DateValue
    fromDFValue (DateValue d) = Right d
    fromDFValue other = Left $ TypeMismatch "Day" (T.pack $ show other)

instance CanBeDFValue UTCTime where
    toDFValue = TimestampValue
    fromDFValue (TimestampValue t) = Right t
    fromDFValue other = Left $ TypeMismatch "UTCTime" (T.pack $ show other)

instance CanBeDFValue Bool where
    toDFValue = BoolValue
    fromDFValue (BoolValue b) = Right b
    fromDFValue other = Left $ TypeMismatch "Bool" (T.pack $ show other)

instance CanBeDFValue a => CanBeDFValue (Maybe a) where
    toDFValue Nothing = NA
    toDFValue (Just a) = toDFValue a
    fromDFValue NA = Right Nothing
    fromDFValue x = case fromDFValue x of
        Right val -> Right (Just val)
        Left err -> Left err

fromDFValueUnsafe :: forall a. CanBeDFValue a => DFValue -> Either SaraError a
fromDFValueUnsafe = fromDFValue

-- | Converts a 'DataFrame' into a list of 'Row's.
-- Each 'Row' is a 'Map' where keys are column names and values are the corresponding 'DFValue's for that row.
toRows :: DataFrame cols -> [Row]
toRows (DataFrame dfMap) =
    if Map.null dfMap
        then []
        else
            let
                numRows = case Map.toList dfMap of
                    [] -> 0
                    (_, col) : _ -> V.length col
                columnNames' = Map.keys dfMap
            in
                [ Map.fromList [ (colName, (dfMap Map.! colName) V.! rowIndex) | colName <- columnNames' ]
                | rowIndex <- [0 .. numRows - 1]
                ]

-- * Type-level list operations

-- | Create a DataFrame from a list of rows.
fromRows :: forall cols. KnownColumns cols => [Row] -> DataFrame cols
fromRows [] = DataFrame Map.empty
fromRows rows =
    let
        -- Get the expected column names from the type-level schema
        expectedColNames = columnNames (Proxy @cols)
        -- For each expected column, create a vector of its values across all rows
        colMap = Map.fromList $ map (\colName ->
            let
                -- For each row, try to find the value for the current column.
                -- If not found, use NA.
                colValues = V.fromList $ map (\row -> Map.findWithDefault NA colName row) rows
            in
                (colName, colValues)
            ) expectedColNames
    in
        DataFrame colMap



-- | A type family to get the type of a column from a schema.
type family TypeOf (s :: Symbol) (cols :: [(Symbol, Type)]) :: Type where
  TypeOf s ('(colName, colType) ': rest) = TypeOfImpl (CmpSymbol s colName) s colType rest
  TypeOf s '[] = TypeError (Text "Column '" :<>: ShowType s :<>: Text "' not found in DataFrame schema.")

type family TypeOfImpl (ord :: Ordering) (s :: Symbol) (colType :: Type) (rest :: [(Symbol, Type)]) :: Type where
  TypeOfImpl 'EQ s colType rest = colType
  TypeOfImpl _ s colType rest = TypeOf s rest

-- | A type family to append two type-level lists.
type family (:++:) (xs :: [k]) (ys :: [k]) :: [k] where
    -- | Appends two type-level lists. Used for combining schemas.
    '[] :++: ys = ys
    (x ': xs) :++: ys = x ': (xs :++: ys)

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
-- | A constraint synonym for checking if a column exists in a DataFrame.
type HasColumn (col :: Symbol) (cols :: [(Symbol, Type)]) = (KnownSymbol col, Typeable (TypeOf col cols))

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
    JoinCols cols1 cols2 = Nub (cols1 :++: cols2)

prop_fromRows_toRows_identity :: (KnownColumns cols, Eq (DataFrame cols), Arbitrary (DataFrame cols)) => DataFrame cols -> Bool
prop_fromRows_toRows_identity df = (fromRows . toRows) df == df