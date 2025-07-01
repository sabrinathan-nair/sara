{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DerivingStrategies #-}

module Sara.DataFrame.Static (
    tableTypes,
    readCsv
) where

import Language.Haskell.TH
import Data.Csv (FromNamedRecord, decodeByName, decode, HasHeader(NoHeader), parseField, NamedRecord)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Vector as V
import Data.Char (toLower)
import GHC.Generics (Generic)
import qualified Data.Map.Strict as M
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE


-- | A Template Haskell function that generates a record type from a CSV file.
-- The first row of the CSV file is used to determine the field names.
-- The types of the fields are inferred from the data in the first data row.
tableTypes :: String -> FilePath -> Q [Dec]
tableTypes name filePath = do
    contents <- runIO $ BL.readFile filePath
    case decode NoHeader contents of
        Left err -> fail err
        Right (records :: V.Vector (V.Vector BC.ByteString)) -> do
            let headers = V.toList $ V.map TE.decodeUtf8 $ records V.! 0
            let firstRow = V.toList $ V.map TE.decodeUtf8 $ records V.! 1
            let fields = zipWith (\h t -> (mkName (T.unpack (normalizeFieldName h)), simpleType t)) headers firstRow
            let recordName = mkName name
            record <- dataD (cxt []) recordName [] Nothing [recC recordName (map (\(n, t) -> varBangType n (bangType (bang noSourceUnpackedness noSourceStrictness) t)) fields)]
              [ derivClause (Just StockStrategy) [conT ''Show, conT ''Generic]
              , derivClause (Just AnyclassStrategy) [conT ''FromNamedRecord]
              ]
            return [record]

-- | Normalizes a field name to be a valid Haskell identifier.
normalizeFieldName :: T.Text -> T.Text
normalizeFieldName t = case T.uncons t of
    Just (x, xs) -> T.cons (toLower x) xs
    Nothing -> T.empty

-- | Infers a simple type from a string value.
simpleType :: T.Text -> Q Type
simpleType s =
    case reads (T.unpack s) :: [(Int, String)] of
        [(n, "")] -> [t| Int |]
        _ -> case reads (T.unpack s) :: [(Double, String)] of
            [(n, "")] -> [t| Double |]
            _ -> [t| T.Text |]

readCsv :: (FromNamedRecord a) => FilePath -> IO (Either String (V.Vector a))
readCsv filePath = do
    contents <- BL.readFile filePath
    let (headerLine, dataLines) = BL.break (== 10) contents -- 10 is newline character

    -- Decode the header line into individual ByteString fields
    case decode NoHeader headerLine of
        Left err -> return $ Left err
        Right records -> -- records is V.Vector (V.Vector ByteString)
            if V.null records
                then return $ Left "Empty header line"
                else do
                    let rawHeaderFields = V.head records -- rawHeaderFields is V.Vector ByteString
                    -- Normalize each header field
                    let normalizedTextHeaders = map (normalizeFieldName . TE.decodeUtf8) (V.toList rawHeaderFields) :: [T.Text]
                    let normalizedByteStringHeaders = map TE.encodeUtf8 normalizedTextHeaders :: [BC.ByteString]

                    -- Reconstruct the normalized header line
                    let normalizedHeaderLine = BL.intercalate (BL.singleton 44) (map BL.fromStrict normalizedByteStringHeaders)

                    -- Reconstruct the full CSV content with the normalized header
                    let newContents = normalizedHeaderLine `BL.append` (BL.singleton 10) `BL.append` dataLines
                    return $ snd <$> decodeByName newContents
