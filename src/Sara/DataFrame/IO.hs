-- Sara.DataFrame.IO module

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

module Sara.DataFrame.IO (
    readCSV,
    writeCSV,
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
import Data.Time (Day, UTCTime)
import Data.Maybe (fromMaybe)
import Text.Read (readMaybe)
import Data.Time.Format (formatTime, parseTimeM, defaultTimeLocale)
import qualified Data.ByteString.Char8 as BC
import qualified Data.HashMap.Strict as HM
import Data.Char (toUpper)
import Data.Aeson as A
import Data.Proxy (Proxy(..))
import Data.Typeable (TypeRep, Typeable, typeRep)
import GHC.TypeLits (Symbol)
import Data.Kind (Type)

import Sara.DataFrame.Types

-- | Attempts to parse a ByteString into a DFValue type.
parseValue :: TypeRep -> BC.ByteString -> DFValue
parseValue expectedType bs
    | BC.null bs = NA
    | bs == (TE.encodeUtf8 . T.pack) "NA" = NA
    | otherwise = 
        let s = TE.decodeUtf8 bs
        in  case () of
                _ | expectedType == typeRep (Proxy @Int) ->
                    case readMaybe (T.unpack s) :: Maybe Int of
                        Just i -> IntValue i
                        Nothing -> error $ "Type mismatch: Expected Int, got " ++ T.unpack s
                _ | expectedType == typeRep (Proxy @Double) ->
                    case readMaybe (T.unpack s) :: Maybe Double of
                        Just d -> DoubleValue d
                        Nothing -> error $ "Type mismatch: Expected Double, got " ++ T.unpack s
                _ | expectedType == typeRep (Proxy @T.Text) -> TextValue s
                _ | expectedType == typeRep (Proxy @Day) ->
                    case parseTimeM True defaultTimeLocale "%Y-%m-%d" (T.unpack s) :: Maybe Day of
                        Just day -> DateValue day
                        Nothing -> error $ "Type mismatch: Expected Day (YYYY-MM-DD), got " ++ T.unpack s
                _ | expectedType == typeRep (Proxy @UTCTime) ->
                    case parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" (T.unpack s) :: Maybe UTCTime of
                        Just t -> TimestampValue t
                        Nothing -> error $ "Type mismatch: Expected UTCTime (YYYY-MM-DDTHH:MM:SSZ), got " ++ T.unpack s
                _ | expectedType == typeRep (Proxy @Bool) ->
                    case T.toLower s of
                        "true" -> BoolValue True
                        "false" -> BoolValue False
                        _ -> error $ "Type mismatch: Expected Bool, got " ++ T.unpack s
                _ -> error $ "Unsupported type: " ++ show expectedType

-- | Converts a DFValue to a ByteString for writing to CSV.
valueToByteString :: DFValue -> BC.ByteString
valueToByteString (IntValue i) = BC.pack (show i)
valueToByteString (DoubleValue d) = BC.pack (show d)
valueToByteString (TextValue t) = TE.encodeUtf8 t
valueToByteString (DateValue d) = BC.pack (formatTime defaultTimeLocale "%Y-%m-%d" d)
valueToByteString (BoolValue b) = BC.pack (map toUpper (show b))
valueToByteString NA = BC.pack "NA"

-- | Reads a CSV file from the given file path and converts it into a DataFrame.
readCSV :: forall (cols :: [(Symbol, Type)]). KnownColumns cols => Proxy cols -> FilePath -> IO (DataFrame cols)
readCSV p filePath = do
    let expectedColNames = columnNames p
        expectedColTypes = columnTypes p
    csvData <- BL.readFile filePath
    case C.decodeByName csvData :: Either String (C.Header, V.Vector C.NamedRecord) of
        Left err -> error $ "CSV parsing error: " ++ err
        Right (header, records) -> do
            if V.null records
                then return $ DataFrame Map.empty
                else do
                    let actualColumnNames = V.map TE.decodeUtf8 header
                    if V.fromList expectedColNames /= actualColumnNames
                        then error $ "CSV header mismatch. Expected: " ++ show expectedColNames ++ ", Got: " ++ show (V.toList actualColumnNames)
                        else do
                            let initialColumnsMap = Map.fromList $ V.toList $ V.map (\colName -> (colName, V.empty)) actualColumnNames
                                expectedColTypeMap = Map.fromList $ zip expectedColNames expectedColTypes
                                finalColumnsMap = V.foldl' (\accMap record ->
                                        Map.mapWithKey (\colName colVec ->
                                            let expectedType = fromMaybe (error $ "Type not found for column: " ++ T.unpack colName) $ Map.lookup colName expectedColTypeMap
                                                val = parseValue expectedType $ fromMaybe BC.empty (HM.lookup (TE.encodeUtf8 colName) record)
                                            in V.snoc colVec val
                                        ) accMap
                                    ) initialColumnsMap records
                            return $ DataFrame finalColumnsMap

-- | Writes a DataFrame to a CSV file at the given file path.
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

-- | Reads a JSON file into a DataFrame.
readJSON :: forall cols. KnownColumns cols => FilePath -> IO (DataFrame cols)
readJSON filePath = do
    jsonData <- BL.readFile filePath
    case A.eitherDecode jsonData :: Either String [Map T.Text DFValue] of
        Left err -> error $ "JSON parsing error: " ++ err
        Right rows -> do
            if null rows
                then return $ DataFrame Map.empty
                else
                    let
                        -- Assuming all rows have the same columns
                        columnNames = Map.keys (head rows)
                        -- Convert list of rows to Map of columns
                        dfMap = Map.fromList $ map (\colName ->
                            (colName, V.fromList $ map (\row -> Map.findWithDefault NA colName row) rows)
                            ) columnNames
                    in
                        return $ DataFrame dfMap

-- | Writes a DataFrame to a JSON file.
writeJSON :: KnownColumns cols => FilePath -> DataFrame cols -> IO ()
writeJSON filePath df = do
    let rows = toRows df
    BL.writeFile filePath (A.encode rows)
