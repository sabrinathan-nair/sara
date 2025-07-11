-- Sara.DataFrame.IO module

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleInstances #-}

module Sara.DataFrame.IO (
    readCsv,
    writeCSV,
    readJSON,
    writeJSON,
    validateDFValue
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
import Data.Time.Format (formatTime, defaultTimeLocale)
import qualified Data.ByteString.Char8 as BC
import qualified Data.HashMap.Strict as HM
import Data.Char (toUpper)
import Data.Aeson as A
import Data.Proxy (Proxy(..))
import Data.Typeable (TypeRep, typeRep)
import Sara.DataFrame.Types (DFValue(..), DataFrame(..), KnownColumns(..), toRows, isNA)
import Sara.DataFrame.Static (readCsv)



-- FromField instances for DFValue













-- | Converts a DFValue to a ByteString for writing to CSV.
valueToByteString :: DFValue -> BC.ByteString
valueToByteString (IntValue i) = BC.pack (show i)
valueToByteString (DoubleValue d) = BC.pack (show d)
valueToByteString (TextValue t) = TE.encodeUtf8 t
valueToByteString (DateValue d) = BC.pack (formatTime defaultTimeLocale "%Y-%m-%d" d)
valueToByteString (TimestampValue t) = BC.pack (formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" t)
valueToByteString (BoolValue b) = BC.pack (map toUpper (show b))
valueToByteString NA = BC.pack "NA"

validateDFValue :: TypeRep -> DFValue -> DFValue
validateDFValue expectedType val = 
    if typeRep (Proxy @Int) == expectedType && isIntValue val then val
    else if typeRep (Proxy @Double) == expectedType && isDoubleValue val then val
    else if typeRep (Proxy @T.Text) == expectedType && isTextValue val then val
    else if typeRep (Proxy @Day) == expectedType && isDateValue val then val
    else if typeRep (Proxy @UTCTime) == expectedType && isTimestampValue val then val
    else if typeRep (Proxy @Bool) == expectedType && isBoolValue val then val
    else if isNA val then NA
    else error $ "Type mismatch: Expected " ++ show expectedType ++ ", but got " ++ show val

isIntValue :: DFValue -> Bool
isIntValue (IntValue _) = True
isIntValue _ = False

isDoubleValue :: DFValue -> Bool
isDoubleValue (DoubleValue _) = True
isDoubleValue _ = False

isTextValue :: DFValue -> Bool
isTextValue (TextValue _) = True
isTextValue _ = False

isDateValue :: DFValue -> Bool
isDateValue (DateValue _) = True
isDateValue _ = False

isTimestampValue :: DFValue -> Bool
isTimestampValue (TimestampValue _) = True
isTimestampValue _ = False

isBoolValue :: DFValue -> Bool
isBoolValue (BoolValue _) = True
isBoolValue _ = False

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
readJSON :: forall cols. KnownColumns cols => Proxy cols -> FilePath -> IO (DataFrame cols)
readJSON p filePath = do
    let expectedColNames = columnNames p
        expectedColTypes = columnTypes p
        expectedColTypeMap = Map.fromList $ zip expectedColNames expectedColTypes

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
                                            let expectedType = fromMaybe (error $ "Type not found for column: " ++ T.unpack colName) $ Map.lookup colName expectedColTypeMap
                                                val = fromMaybe NA (Map.lookup colName row) -- Get DFValue from row
                                            in V.snoc colVec (validateDFValue expectedType val) -- Validate DFValue against expectedType
                                        ) accMap
                                    ) initialColumnsMap (V.fromList rows) -- Convert rows to Vector for foldl'

                            return $ DataFrame finalColumnsMap



-- | Writes a DataFrame to a JSON file.
writeJSON :: KnownColumns cols => FilePath -> DataFrame cols -> IO ()
writeJSON filePath df = do
    let rows = toRows df
    BL.writeFile filePath (A.encode rows)
