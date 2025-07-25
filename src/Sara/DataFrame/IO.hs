{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections #-}

-- | This module provides functions for reading from and writing to common
-- data formats like CSV and JSON. It ensures that the data conforms to the
-- type-level schema of the `DataFrame`.
module Sara.DataFrame.IO (
    -- * CSV Functions
    readCsv,
    writeCSV,
    readCsvStreaming,
    -- * JSON Functions
    readJSON,
    readJSONStreaming,
    writeJSON,
    writeJSONStreaming
) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as C
import Data.Csv (FromNamedRecord)
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
import GHC.Generics (Generic, Rep)

import Sara.DataFrame.Types (DFValue(..), DataFrame(..), KnownColumns(..), toRows)
import Sara.DataFrame.Static (readCsv)
import Sara.DataFrame.Internal (HasSchema, Schema, recordToDFValueList, GToFields)

import Streaming (Stream, Of)
import qualified Streaming.Prelude as S
import Control.Monad.IO.Class (liftIO)

-- | Reads a CSV file in a streaming fashion.
-- It returns a `Stream` of `DataFrame`s, where each `DataFrame` contains a single row.
readCsvStreaming :: forall record cols. (FromNamedRecord record, HasSchema record, cols ~ Schema record, KnownColumns cols, Generic record, GToFields (Rep record)) => Proxy record -> FilePath -> Stream (Of (DataFrame cols)) IO ()
readCsvStreaming _ filePath = do
    let expectedColNames = V.fromList $ columnNames (Proxy @cols)
    contents <- liftIO $ BL.readFile filePath
    case C.decodeByName contents :: Either String (C.Header, V.Vector record) of
        Left err -> liftIO $ ioError $ userError $ "CSV parsing error: " ++ err
        Right (header, records) -> do
            let actualHeader = V.map TE.decodeUtf8 header
            if actualHeader /= expectedColNames
                then liftIO $ ioError $ userError $ "CSV header mismatch. Expected: " ++ show expectedColNames ++ ", Got: " ++ show actualHeader
                else do
                    S.for (S.each (V.toList records)) $ \record -> do
                        let rowMap = Map.fromList $ V.toList $ V.zip actualHeader (V.fromList (recordToDFValueList record))
                        let df = DataFrame (Map.map V.singleton rowMap)
                        S.yield df


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
                            let initialColumnsMap = Map.fromList $ V.toList $ V.map (, V.empty) (V.fromList actualColumnNames)
                                finalColumnsMap = V.foldl' (\accMap row ->
                                        Map.mapWithKey (\colName colVec ->
                                            let val = fromMaybe NA (Map.lookup colName row) -- Get DFValue from row
                                            in V.snoc colVec val
                                        ) accMap
                                    ) initialColumnsMap (V.fromList rows) -- Convert rows to Vector for foldl'

                            return $ DataFrame finalColumnsMap

-- | Reads a JSON file in a streaming fashion.
-- NOTE: This is a temporary non-streaming implementation due to issues with streaming JSON libraries.
-- It reads the entire file into memory before processing.
readJSONStreaming :: forall cols. KnownColumns cols => Proxy cols -> FilePath -> Stream (Of (DataFrame cols)) IO ()
readJSONStreaming _ filePath = do
    jsonData <- liftIO $ BL.readFile filePath
    case A.eitherDecode jsonData :: Either String [Map T.Text DFValue] of
        Left err -> liftIO $ ioError $ userError $ "JSON parsing error: " ++ err
        Right rows -> do
            S.for (S.each rows) $ \row -> do
                let rowMap = Map.map V.singleton row
                S.yield (DataFrame rowMap)

-- | Writes a `DataFrame` to a JSON file.
-- The output is an array of objects, where each object represents a row.
writeJSON :: KnownColumns cols => FilePath -> DataFrame cols -> IO ()
writeJSON filePath df = do
    let rows = toRows df
    BL.writeFile filePath (A.encode rows)

-- | Writes a `DataFrame` to a JSON file in a streaming fashion.
writeJSONStreaming :: KnownColumns cols => FilePath -> Stream (Of (DataFrame cols)) IO () -> IO ()
writeJSONStreaming filePath dfStream = do
    dfs <- S.toList_ dfStream
    let allRows = concatMap toRows dfs
    BL.writeFile filePath (A.encode (map A.toJSON allRows))