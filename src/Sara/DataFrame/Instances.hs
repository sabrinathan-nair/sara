{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeApplications #-}

-- | This module provides `FromField` instances for various types used in `DFValue`.
-- These instances are used by the `cassava` library to parse CSV data.
module Sara.DataFrame.Instances where

import qualified Data.Csv as C
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time (Day, UTCTime)
import Data.Time.Format (parseTimeM, defaultTimeLocale)
import Control.Applicative ((<|>))
import Sara.DataFrame.Types (DFValue(..))

-- | Parses a `Bool` from a CSV field. Accepts "true" or "false" (case-insensitive).
instance C.FromField Bool where
    parseField s
        | T.toLower (TE.decodeUtf8 s) == "true" = return True
        | T.toLower (TE.decodeUtf8 s) == "false" = return False
        | otherwise = fail "Not a boolean value"

-- | Parses a `Day` from a CSV field. Expects the format "YYYY-MM-DD".
instance C.FromField Day where
    parseField s = parseTimeM True defaultTimeLocale "%Y-%m-%d" (T.unpack (TE.decodeUtf8 s))

-- | Parses a `UTCTime` from a CSV field. Expects the format "YYYY-MM-DDTHH:MM:SSZ".
instance C.FromField UTCTime where
    parseField s = parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" (T.unpack (TE.decodeUtf8 s))

-- | Parses a `DFValue` from a CSV field.
-- It tries to parse the field in the following order:
--
-- 1.  `NA` (if the field is "NA" or empty)
-- 2.  `IntValue`
-- 3.  `DoubleValue`
-- 4.  `BoolValue`
-- 5.  `DateValue`
-- 6.  `TimestampValue`
-- 7.  `TextValue` (as a fallback)
instance C.FromField DFValue where
    parseField s
        | s == "NA" || s == "" = return NA
        | otherwise = 
            (IntValue <$> C.parseField s) <|> 
            (DoubleValue <$> C.parseField s) <|> 
            (BoolValue <$> C.parseField s) <|> 
            (DateValue <$> C.parseField s) <|> 
            (TimestampValue <$> C.parseField s) <|> 
            (return (TextValue (TE.decodeUtf8 s)))