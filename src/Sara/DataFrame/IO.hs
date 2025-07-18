-- Sara.DataFrame.IO module

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}

-- | This module provides functions for reading from and writing to common
-- data formats like CSV and JSON. It ensures that the data conforms to the
-- type-level schema of the `DataFrame`.
module Sara.DataFrame.IO (
    -- * CSV Functions
    readCsv,
    writeCSV,
    -- * JSON Functions
    readJSON,
    writeJSON
) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as C
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

import Data.Maybe (fromMaybe)
import Data.Time.Format (formatTime, defaultTimeLocale)
import qualified Data.ByteString.Char8 as BC
import qualified Data.HashMap.Strict as HM
import Data.Char (toUpper)
import Data.Aeson as A
import Data.Proxy (Proxy(..))

import Sara.DataFrame.Types (DFValue(..), DataFrame(..), KnownColumns(..), toRows)
import Sara.DataFrame.Static (readCsv)

-- | Converts a `DFValue` to a `ByteString` for writing to a CSV file.
valueToByteString :: DFValue -> BC.ByteString
valueToByteString (IntValue i) = BC.pack (show i)
valueToByteString (DoubleValue d) = BC.pack (show d)
valueToByteString (TextValue t) = TE.encodeUtf8 t
valueToByteString (DateValue d) = BC.pack (formatTime defaultTimeLocale "%Y-%m-%d" d)
valueToByteString (TimestampValue t) = BC.pack (formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" t)
valueToByteString (BoolValue b) = BC.pack (map toUpper (show b))
valueToByteString NA = BC.pack "NA"



-- | Writes a `DataFrame` to a CSV file.
-- The header is derived from the `DataFrame`'s column names.
writeCSV :: KnownColumns cols => FilePath -> DataFrame cols -> IO ()
writeCSV filePath (DataFrame dfMap) = do
    let header = V.fromList $ Map.keys dfMap
        headerBS = V.map TE.encodeUtf8 header -- Convert Text header to ByteString

        -- Get all columns and ensure they have the same length
        columns = Map.elems dfMap
        numRows = if null columns then 0 else V.length (head columns)

        -- Create NamedRecords from rows
        rows = V.generate numRows $ \rowIndex ->
            let rowValues = V.map (\colName -> valueToByteString $ (dfMap Map.! colName) V.! rowIndex) header
            in C.namedRecord $ HM.toList $ HM.fromList $ V.toList $ V.zip headerBS rowValues

    BL.writeFile filePath $ C.encodeByName headerBS (V.toList rows)

-- | Reads a JSON file into a `DataFrame`.
-- The JSON file should be an array of objects, where each object represents a row.
-- The keys of the objects should match the column names of the `DataFrame`.
-- The function validates that the column names and types in the JSON file match the `DataFrame`'s schema.
readJSON :: forall cols. KnownColumns cols => Proxy cols -> FilePath -> IO (DataFrame cols)
readJSON p filePath = do
    let expectedColNames = columnNames p

    jsonData <- BL.readFile filePath
    case A.eitherDecode jsonData :: Either String [Map T.Text DFValue] of
        Left err -> error $ "JSON parsing error: " ++ err
        Right rows -> do
            if null rows
                then return $ DataFrame Map.empty
                else do
                    let actualColumnNames = Map.keys (head rows)
                    -- Validate column names
                    if V.fromList expectedColNames /= V.fromList actualColumnNames
                        then error $ "JSON header mismatch. Expected: " ++ show expectedColNames ++ ", Got: " ++ show actualColumnNames
                        else do
                            let initialColumnsMap = Map.fromList $ V.toList $ V.map (\colName -> (colName, V.empty)) (V.fromList actualColumnNames)
                                finalColumnsMap = V.foldl' (\accMap row ->
                                        Map.mapWithKey (\colName colVec ->
                                            let val = fromMaybe NA (Map.lookup colName row) -- Get DFValue from row
                                            in V.snoc colVec val
                                        ) accMap
                                    ) initialColumnsMap (V.fromList rows) -- Convert rows to Vector for foldl'

                            return $ DataFrame finalColumnsMap



-- | Writes a `DataFrame` to a JSON file.
-- The output is an array of objects, where each object represents a row.
writeJSON :: KnownColumns cols => FilePath -> DataFrame cols -> IO ()
writeJSON filePath df = do
    let rows = toRows df
    BL.writeFile filePath (A.encode rows)
