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

import Data.Maybe (fromMaybe, listToMaybe)

import qualified Data.ByteString.Char8 as BC
import qualified Data.HashMap.Strict as HM
import Data.Char (toUpper)
import Data.Aeson as A
import Data.Proxy (Proxy(..))
import GHC.Generics (Generic, Rep)

import Sara.DataFrame.Types (DFValue(..), DataFrame(..), KnownColumns(..), toRows, fromRows)
import Sara.DataFrame.Static (readCsv)
import Sara.DataFrame.Internal (HasSchema, Schema, recordToDFValueList, GToFields)
import Sara.Error (SaraError(..))

import Streaming (Stream, Of)
import qualified Streaming.Prelude as S
import Control.Exception (try, IOException)


import Sara.Validation.Employee (validateEmployee, ValidatedEmployee(..))
import Sara.Schema.Definitions (EmployeesRecord)

import Data.Bifunctor (first)
import Data.Time.Format (formatTime, defaultTimeLocale)
import Sara.Core.Types (unEmployeeID, unSalary)





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


-- | Reads a CSV file in a streaming fashion.
-- It returns a `Stream` of `DataFrame`s, where each `DataFrame` contains a single row.
readCsvStreaming :: forall record proxy cols. (FromNamedRecord record, HasSchema record, cols ~ Schema record, KnownColumns cols, Generic record, GToFields (Rep record)) => proxy record -> FilePath -> IO (Either [SaraError] (Stream (Of (Either SaraError (DataFrame cols))) IO ()))
readCsvStreaming _ filePath = do
    eContents <- try (BL.readFile filePath) :: IO (Either IOException BL.ByteString)
    case eContents of
        Left e -> return $ Left $ [IOError (T.pack $ show e)]
        Right contents -> return $ case first (pure . ParsingError . T.pack) (C.decodeByName contents) :: Either [SaraError] (C.Header, V.Vector EmployeesRecord) of 
            Left errs -> Left errs
            Right (header, records) -> 
                let expectedColNames = V.fromList $ columnNames (Proxy @cols)
                    actualHeader = V.map TE.decodeUtf8 header
                in if actualHeader /= expectedColNames
                    then Left $ [IOError (T.pack $ "CSV header mismatch. Expected: " ++ show expectedColNames ++ ", Got: " ++ show actualHeader)]
                    else Right $ S.for (S.each (V.toList records)) $ \rec ->
                        let dfValues = recordToDFValueList rec
                        in case dfValues of
                            [IntValue eid', TextValue n', TextValue dn', DoubleValue s', DateValue sd', TextValue e'] ->
                                case validateEmployee (fromIntegral eid', n', dn', s', sd', e') of
                                    Left errs -> S.yield (Left (GenericError (T.pack $ show errs)))
                                    Right validated -> do
                                        let rowMap = Map.fromList
                                                [ ("employeeID", IntValue (unEmployeeID (veEmployeeID validated))) 
                                                , ("name", TextValue (veName validated))
                                                , ("departmentName", TextValue (T.pack $ show (veDepartmentName validated)))
                                                , ("salary", DoubleValue (unSalary (veSalary validated)))
                                                , ("startDate", DateValue (veStartDate validated))
                                                , ("email", TextValue (T.pack $ show (veEmail validated)))
                                                ]
                                        let df = DataFrame (Map.map V.singleton rowMap)
                                        S.yield (Right df)
                            _ -> S.yield (Left (GenericError "Mismatched EmployeesRecord fields"))



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
        numRows = case listToMaybe columns of
            Just c -> V.length c
            Nothing -> 0

        -- Create NamedRecords from rows
        rows = V.generate numRows $ \rowIndex ->
            let rowValues = V.map (\colName -> valueToByteString $ fromMaybe NA (Map.lookup colName dfMap >>= (\colVec -> colVec V.!? rowIndex))) header
            in C.namedRecord $ HM.toList $ HM.fromList $ V.toList $ V.zip headerBS rowValues

    BL.writeFile filePath $ C.encodeByName headerBS (V.toList rows)



-- | Reads a JSON file into a `DataFrame`.
-- The JSON file should be an array of objects, where each object represents a row.
-- The keys of the objects should match the column names of the `DataFrame`.
-- The function validates that the column names and types in the JSON file match the `DataFrame`'s schema.
readJSON :: forall cols. KnownColumns cols => Proxy cols -> FilePath -> IO (Either [SaraError] (DataFrame cols))
readJSON _ filePath = do
    jsonData <- BL.readFile filePath
    case first (pure . ParsingError . T.pack) (A.eitherDecode jsonData) :: Either [SaraError] [EmployeesRecord] of
        Left err -> return $ Left err
        Right records -> do
            let validatedRecords =
                    traverse
                        (\rec ->
                            let dfValues = recordToDFValueList rec
                            in case dfValues of
                                [IntValue eid', TextValue n', TextValue dn', DoubleValue s', DateValue sd', TextValue e'] ->
                                    validateEmployee (fromIntegral eid', n', dn', s', sd', e')
                                _ ->
                                    Left [GenericError "Mismatched EmployeesRecord fields"]
                        )
                        records
            case validatedRecords of 
                Left errs -> return $ Left errs
                Right validated -> do
                    let rows = map (\v -> Map.fromList
                                        [ ("employeeID", IntValue (unEmployeeID (veEmployeeID v))) 
                                        , ("name", TextValue (veName v))
                                        , ("departmentName", TextValue (T.pack $ show (veDepartmentName v)))
                                        , ("salary", DoubleValue (unSalary (veSalary v)))
                                        , ("startDate", DateValue (veStartDate v))
                                        , ("email", TextValue (T.pack $ show (veEmail v)))
                                        ]) validated
                    return $ Right $ fromRows rows



-- | Reads a JSON file in a streaming fashion.
-- NOTE: This is a temporary non-streaming implementation due to issues with streaming JSON libraries.
-- It reads the entire file into memory before processing.
readJSONStreaming :: forall cols. KnownColumns cols => Proxy cols -> FilePath -> IO (Either [SaraError] (Stream (Of (DataFrame cols)) IO ()))
readJSONStreaming _ filePath = do
    eJsonData <- try (BL.readFile filePath) :: IO (Either IOException BL.ByteString)
    case eJsonData of
        Left e -> return $ Left $ [IOError (T.pack $ show e)]
        Right jsonData -> case first (pure . ParsingError . T.pack) (A.eitherDecode jsonData) :: Either [SaraError] [Map T.Text DFValue] of 
            Left err -> return $ Left err
            Right rows -> return $ Right $ S.for (S.each rows) $ \row -> 
                let rowMap = Map.map V.singleton row
                in S.yield (DataFrame rowMap)


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
