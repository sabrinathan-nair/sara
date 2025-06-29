-- Sara.DataFrame.IO module

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
import Data.Time (Day)
import Data.Maybe (fromMaybe)
import Text.Read (readMaybe)
import Data.Time.Format (formatTime, parseTimeM, defaultTimeLocale)
import qualified Data.ByteString.Char8 as BC
import qualified Data.HashMap.Strict as HM
import Data.Char (toUpper)
import Data.Aeson as A

import Sara.DataFrame.Types

-- | Attempts to parse a ByteString into a DFValue type.
parseValue :: BC.ByteString -> DFValue
parseValue bs
    | BC.null bs = NA
    | bs == (TE.encodeUtf8 . T.pack) "NA" = NA
    | bs == (TE.encodeUtf8 . T.pack) "TRUE" = BoolValue True
    | bs == (TE.encodeUtf8 . T.pack) "FALSE" = BoolValue False
    | otherwise = 
        let s = TE.decodeUtf8 bs
        in  case (readMaybe (T.unpack s) :: Maybe Int) of
                Just i -> IntValue i
                Nothing -> case (readMaybe (T.unpack s) :: Maybe Double) of
                    Just d -> DoubleValue d
                    Nothing -> case parseTimeM True defaultTimeLocale "%Y-%m-%d" (T.unpack s) :: Maybe Day of
                        Just day -> DateValue day
                        Nothing -> TextValue s

-- | Converts a DFValue to a ByteString for writing to CSV.
valueToByteString :: DFValue -> BC.ByteString
valueToByteString (IntValue i) = BC.pack (show i)
valueToByteString (DoubleValue d) = BC.pack (show d)
valueToByteString (TextValue t) = TE.encodeUtf8 t
valueToByteString (DateValue d) = BC.pack (formatTime defaultTimeLocale "%Y-%m-%d" d)
valueToByteString (BoolValue b) = BC.pack (map toUpper (show b))
valueToByteString NA = BC.pack "NA"

-- | Reads a CSV file from the given file path and converts it into a DataFrame.
readCSV :: FilePath -> IO DataFrame
readCSV filePath = do
    csvData <- BL.readFile filePath
    case C.decodeByName csvData :: Either String (C.Header, V.Vector C.NamedRecord) of
        Left err -> error $ "CSV parsing error: " ++ err
        Right (header, records) -> do
            if V.null records
                then return $ DataFrame Map.empty
                else do
                    let columnNames = V.map TE.decodeUtf8 header
                        -- Initialize an empty map for columns
                        initialColumnsMap = Map.fromList $ V.toList $ V.map (\colName -> (colName, V.empty)) columnNames

                        -- Fold over records to build columns
                        finalColumnsMap = V.foldl' (\accMap record ->
                                Map.mapWithKey (\colName colVec ->
                                    let val = parseValue $ fromMaybe BC.empty (HM.lookup (TE.encodeUtf8 colName) record)
                                    in V.snoc colVec val
                                ) accMap
                            ) initialColumnsMap records

                    return $ DataFrame finalColumnsMap

-- | Writes a DataFrame to a CSV file at the given file path.
writeCSV :: FilePath -> DataFrame -> IO ()
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
readJSON :: FilePath -> IO DataFrame
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
writeJSON :: FilePath -> DataFrame -> IO ()
writeJSON filePath df = do
    let rows = toRows df
    BL.writeFile filePath (A.encode rows)