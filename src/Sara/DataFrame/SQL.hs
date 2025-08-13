{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
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
import Control.Monad (forM)
import Control.Exception (try, SomeException)
import Data.Maybe (mapMaybe)


import Sara.DataFrame.Types
import Sara.Error (SaraError(..))


-- | Converts a `SQLData` value to a `DFValue`, validating against an expected `TypeRep`.
sqlDataToDFValue :: TypeRep -> SQLData -> Either SaraError DFValue
sqlDataToDFValue expectedType sqlData =
    case sqlData of
        SQLInteger i
            | expectedType == typeRep (Proxy @Int) -> Right $ IntValue (fromIntegral i)
            | expectedType == typeRep (Proxy @Double) -> Right $ DoubleValue (fromIntegral i)
            | otherwise -> Left $ TypeMismatch (T.pack $ show expectedType) (T.pack $ "SQLInteger " ++ show i)
        SQLFloat d
            | expectedType == typeRep (Proxy @Double) -> Right $ DoubleValue d
            | otherwise -> Left $ TypeMismatch (T.pack $ show expectedType) (T.pack $ "SQLFloat " ++ show d)
        SQLText t
            | expectedType == typeRep (Proxy @T.Text) -> Right $ TextValue t
            | expectedType == typeRep (Proxy @Bool) ->
                case T.toLower t of
                    "true" -> Right $ BoolValue True
                    "false" -> Right $ BoolValue False
                    _ -> Left $ TypeMismatch (T.pack "Bool") (T.pack $ "SQLText " ++ show t)
            | otherwise -> Left $ TypeMismatch (T.pack $ show expectedType) (T.pack $ "SQLText " ++ show t)
        SQLBlob _ -> Left $ GenericError (T.pack "Unsupported SQL type: BLOB")
        SQLNull -> Right NA

-- | Reads data from a SQLite database into a `DataFrame`.
-- The `cols` type parameter specifies the schema of the resulting `DataFrame`.
-- The function validates that the number of columns in the query result matches the schema.
-- It also validates that the types of the values in the query result match the schema.
readSQL :: forall cols. KnownColumns cols => Proxy cols -> FilePath -> Query -> IO (Either SaraError (DataFrame cols))
readSQL p dbPath sqlQuery = do
    result <- try (do
        conn <- open dbPath
        rows <- query_ conn sqlQuery :: IO [[SQLData]]
        close conn
        return rows) :: IO (Either SomeException [[SQLData]])

    case result of
        Left e -> return $ Left $ GenericError (T.pack $ show e)
        Right rows -> do
            let expectedColNames = columnNames p
                colCount = length expectedColNames
                expectedColTypes = columnTypes p

            case rows of
                [] -> return $ Right $ DataFrame Map.empty
                (firstRow:_) -> do
                    if length firstRow /= colCount
                        then return $ Left $ GenericError (T.pack $ "SQL query result column count mismatch. Expected " ++ show colCount ++ ", got " ++ show (length firstRow))
                        else do
                            let processedRowsResult :: Either SaraError [[DFValue]]
                                processedRowsResult = forM rows $ \row ->
                                    if length row /= colCount
                                        then Left $ GenericError (T.pack $ "Inconsistent column count in SQL result. Expected " ++ show colCount ++ ", got " ++ show (length row))
                                        else forM (zip expectedColTypes row) $ uncurry sqlDataToDFValue

                            case processedRowsResult of
                                Left err -> return $ Left err
                                Right processedRows -> do
                                    let columns = transpose processedRows
                                    let finalColumnsMap = Map.fromList $ zip expectedColNames (map V.fromList columns)
                                    return $ Right $ DataFrame finalColumnsMap

transpose :: [[a]] -> [[a]]
transpose [] = []
transpose ([]:_) = []
transpose xss =
    let safeHead (y:_) = Just y
        safeHead _ = Nothing
        safeTail (_:ys) = Just ys
        safeTail _ = Nothing
    in case traverse safeHead xss of
        Nothing -> []
        Just hs -> hs : transpose (mapMaybe safeTail xss)
