{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Sara.DataFrame.CsvInstances (
) where

import qualified Data.Csv as C
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time (Day, UTCTime)
import Data.Time.Format (parseTimeM, defaultTimeLocale)

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
