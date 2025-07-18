{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

-- | This module provides functions for reading data from a SQL database into a `DataFrame`.
-- It currently supports SQLite.
module Sara.DataFrame.SQL (
    readSQL
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import Database.SQLite.Simple
import Data.Proxy (Proxy(..))
import Data.Typeable (TypeRep, typeRep)
import Data.Maybe (fromMaybe)

import Sara.DataFrame.Types


-- | Converts a `SQLData` value to a `DFValue`, validating against an expected `TypeRep`.
sqlDataToDFValue :: TypeRep -> SQLData -> DFValue
sqlDataToDFValue expectedType sqlData =
    case sqlData of
        SQLInteger i
            | expectedType == typeRep (Proxy @Int) -> IntValue (fromIntegral i)
            | expectedType == typeRep (Proxy @Double) -> DoubleValue (fromIntegral i)
            | otherwise -> error $ "Type mismatch: Expected " ++ show expectedType ++ ", got SQLInteger " ++ show i
        SQLFloat d
            | expectedType == typeRep (Proxy @Double) -> DoubleValue d
            | otherwise -> error $ "Type mismatch: Expected " ++ show expectedType ++ ", got SQLFloat " ++ show d
        SQLText t
            | expectedType == typeRep (Proxy @T.Text) -> TextValue t
            | expectedType == typeRep (Proxy @Bool) ->
                case T.toLower t of
                    "true" -> BoolValue True
                    "false" -> BoolValue False
                    _ -> error $ "Type mismatch: Expected Bool, got SQLText " ++ show t
            | otherwise -> error $ "Type mismatch: Expected " ++ show expectedType ++ ", got SQLText " ++ show t
        SQLBlob _ -> error "Unsupported SQL type: BLOB"
        SQLNull -> NA

-- | Reads data from a SQLite database into a `DataFrame`.
-- The `cols` type parameter specifies the schema of the resulting `DataFrame`.
-- The function validates that the number of columns in the query result matches the schema.
-- It also validates that the types of the values in the query result match the schema.
readSQL :: forall cols. KnownColumns cols => Proxy cols -> FilePath -> Query -> IO (DataFrame cols)
readSQL p dbPath sqlQuery = do
    conn <- open dbPath
    rows <- query_ conn sqlQuery :: IO [[SQLData]] -- Fetch as list of lists of SQLData
    close conn

    let expectedColNames = columnNames p
        expectedColTypes = columnTypes p
        expectedColTypeMap = Map.fromList $ zip expectedColNames expectedColTypes

    if null rows
        then return $ DataFrame Map.empty
        else do
            let numExpectedCols = length expectedColNames
            let firstRowData = head rows -- Assuming all rows have the same number of columns
            let numActualCols = length firstRowData

            if numExpectedCols /= numActualCols
                then error $ "SQL query result column count mismatch. Expected " ++ show numExpectedCols ++ ", got " ++ show numActualCols
                else do
                    let initialColumnsMap = Map.fromList $ V.toList $ V.map (\colName -> (colName, V.empty)) (V.fromList expectedColNames)
                    let finalColumnsMap = V.foldl' (\accMap rowDataList ->
                                let rowData = V.fromList rowDataList -- Convert rowDataList to Vector for safe access
                                in Map.mapWithKey (\colName colVec ->
                                    let expectedType = fromMaybe (error $ "Type not found for column: " ++ T.unpack colName) $ Map.lookup colName expectedColTypeMap
                                        colIndex = fromMaybe (error $ "Internal error: Column " ++ T.unpack colName ++ " not found in expected names list.") $ V.elemIndex colName (V.fromList expectedColNames)
                                    in case rowData V.!? colIndex of
                                        Just sqlData -> V.snoc colVec (sqlDataToDFValue expectedType sqlData)
                                        Nothing -> error $ "Internal error: Column index out of bounds for " ++ T.unpack colName
                                ) accMap
                            ) initialColumnsMap (V.fromList rows)
                    return $ DataFrame finalColumnsMap
