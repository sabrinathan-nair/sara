{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Sara.DataFrame.Types (
    DFValue(..),
    Column,
    DataFrame(..),
    Row,
    toRows,
    SortOrder(..),
    ConcatAxis(..),
    JoinType(..)
) where

import qualified Data.Text as T
import Data.Time (Day, UTCTime)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Aeson
import Data.Aeson.Types (Parser)
import Data.Scientific (toRealFloat)
import Data.Time.Format (formatTime, parseTimeM, defaultTimeLocale)
import Text.Read (readMaybe)

-- | A type to represent a single value in a DataFrame.
-- It can hold different types of data such as integers, doubles, text, dates, booleans, or missing values (NA).
data DFValue = IntValue Int
           | DoubleValue Double
           | TextValue T.Text
           | DateValue Day
           | TimestampValue UTCTime
           | BoolValue Bool
           | NA -- ^ Represents a missing value.
           deriving (Show, Eq, Ord)

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
newtype DataFrame = DataFrame (Map T.Text Column)
    deriving (Show, Eq)

-- | Specifies the sort order for a column.
data SortOrder = Ascending  -- ^ Sort in ascending order.
               | Descending -- ^ Sort in descending order.
    deriving (Show, Eq)

-- | Specifies the axis along which to concatenate DataFrames.
data ConcatAxis = ConcatRows    -- ^ Concatenate DataFrames row-wise.
                | ConcatColumns -- ^ Concatenate DataFrames column-wise.
    deriving (Show, Eq)

-- | Specifies the type of join to perform when merging DataFrames.
data JoinType = InnerJoin   -- ^ Return only the rows that have matching keys in both DataFrames.
              | LeftJoin    -- ^ Return all rows from the left DataFrame, and the matched rows from the right DataFrame.
              | RightJoin   -- ^ Return all rows from the right DataFrame, and the matched rows from the left DataFrame.
              | OuterJoin   -- ^ Return all rows when there is a match in one of the DataFrames.
    deriving (Show, Eq)

-- | Converts a 'DataFrame' into a list of 'Row's.
-- Each 'Row' is a 'Map' where keys are column names and values are the corresponding 'DFValue's for that row.
toRows :: DataFrame -> [Row]
toRows (DataFrame dfMap) =
    if Map.null dfMap
        then []
        else
            let
                -- Assuming all columns have the same number of rows
                numRows = V.length (snd . head . Map.toList $ dfMap)
                columnNames = Map.keys dfMap
            in
                [ Map.fromList [ (colName, (dfMap Map.! colName) V.! rowIndex) | colName <- columnNames ]
                | rowIndex <- [0 .. numRows - 1]
                ]