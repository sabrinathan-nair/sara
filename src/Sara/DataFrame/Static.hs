-- | This module provides Template Haskell functions for statically inferring schemas
-- from CSV files. This allows for creating `DataFrame`s with compile-time guarantees
-- about column names and types.
module Sara.DataFrame.Static (
    -- * Template Haskell Functions
    tableTypes,
    inferCsvSchema,
    -- * CSV Reading
    readCsv,
) where

import Language.Haskell.TH
import Data.Csv (FromNamedRecord, decodeByName, HasHeader(NoHeader))
import qualified Data.ByteString.Lazy as BL
import qualified Data.Vector as V
import Data.Char (toLower)
import GHC.Generics (Generic)
import qualified Data.ByteString.Char8 as BC
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Csv as C
import Data.Time (Day, UTCTime)
import Data.Time.Format (parseTimeM, defaultTimeLocale)
import Text.Read (readMaybe)
import Sara.DataFrame.Internal (HasSchema(..))
import Sara.DataFrame.Instances ()


-- | A Template Haskell function that generates a record type from a CSV file.
-- The first row of the CSV file is used to determine the field names.
-- The types of the fields are inferred from the data in the first data row.
--
-- For example, given a CSV file `people.csv`:
--
-- > name,age
-- > Alice,25
-- > Bob,30
--
-- `tableTypes "Person" "people.csv"` will generate:
--
-- > data Person = Person { name :: T.Text, age :: Int } deriving (Show, Generic)
-- > instance FromNamedRecord Person
tableTypes :: String -> FilePath -> Q [Dec]
tableTypes name filePath = do
    contents <- runIO $ BL.readFile filePath
    case C.decode C.NoHeader contents of
        Left err -> fail err
        Right (records :: V.Vector (V.Vector BC.ByteString)) -> do
            let headers = V.toList $ V.map TE.decodeUtf8 $ records V.! 0
            let firstRow = V.toList $ V.map TE.decodeUtf8 $ records V.! 1
            let fields = zipWith (\h t -> (mkName (T.unpack (normalizeFieldName h)), inferDFType t)) headers firstRow
            let recordName = mkName name
            record <- dataD (cxt []) recordName [] Nothing [recC recordName (map (\(n, t) -> varBangType n (bangType (bang noSourceUnpackedness noSourceStrictness) t)) fields)]
              [ derivClause (Just StockStrategy) [conT ''Show, conT ''Generic]
              , derivClause (Just AnyclassStrategy) [conT ''FromNamedRecord]
              ]
            return [record]

-- | Normalizes a field name to be a valid Haskell identifier.
-- It converts the first character to lowercase.
normalizeFieldName :: T.Text -> T.Text
normalizeFieldName t = case T.uncons t of
    Just (x, xs) -> T.cons (toLower x) xs
    Nothing -> T.empty

-- | Infers the most specific type from a list of sample strings.
-- If any sample is `NA` or empty, the type will be `Maybe` of the inferred type.
inferColumnTypeFromSamples :: [T.Text] -> Q Language.Haskell.TH.Type
inferColumnTypeFromSamples samples = do
    let nonNaSamples = filter (\s -> T.toLower s /= T.pack "na" && not (T.null s)) samples
    let hasNa = length nonNaSamples < length samples -- If any sample was NA or empty

    let baseTypeQ = if null nonNaSamples
        then [t| T.Text |] -- Default to Text if all are NA/empty
        else inferMostSpecificType nonNaSamples

    if hasNa
        then fmap (AppT (ConT ''Maybe)) baseTypeQ
        else baseTypeQ

-- | Infers the most specific type from a list of non-`NA` sample strings.
inferMostSpecificType :: [T.Text] -> Q Language.Haskell.TH.Type
inferMostSpecificType [] = [t| T.Text |] -- Should not happen if called from inferColumnTypeFromSamples
inferMostSpecificType (s:_) = inferDFType s

-- | Infers a `DFValue` type from a string value.
-- The inference order is: `Int`, `Double`, `Day`, `UTCTime`, `Bool`, `T.Text`.
inferDFType :: T.Text -> Q Language.Haskell.TH.Type
inferDFType s
  | Just (_ :: Int) <- readMaybe (T.unpack s) = [t| Int |]
  | Just (_ :: Double) <- readMaybe (T.unpack s) = [t| Double |]
  | Just (_ :: Day) <- parseTimeM True defaultTimeLocale "%Y-%m-%d" (T.unpack s) = [t| Day |]
  | Just (_ :: UTCTime) <- parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" (T.unpack s) = [t| UTCTime |]
  | T.toLower s == T.pack "true" || T.toLower s == T.pack "false" = [t| Bool |]
  | otherwise = [t| T.Text |]

-- | A Template Haskell function that infers a type-level schema from a CSV file
-- and generates a type synonym for it.
-- It also generates a concrete record type for CSV parsing and a `HasSchema` instance.
--
-- For example, given a CSV file `people.csv`:
--
-- > name,age
-- > Alice,25
-- > Bob,30
--
-- `inferCsvSchema "PeopleSchema" "people.csv"` will generate:
--
-- > type PeopleSchema = '[ "name" ::: T.Text, "age" ::: Int ]
-- > data PeopleSchemaRecord = PeopleSchemaRecord { peopleSchemaname :: T.Text, peopleSchemaage :: Int } ...
-- > instance HasSchema PeopleSchemaRecord where type Schema PeopleSchemaRecord = PeopleSchema
inferCsvSchema :: String -> FilePath -> Q [Dec]
inferCsvSchema typeName filePath = do
    contents <- runIO $ BL.readFile filePath
    case C.decode C.NoHeader contents :: Either String (V.Vector (V.Vector BC.ByteString)) of
        Left err -> fail err
        Right records -> do
            if V.null records || V.length records < 2
                then fail "CSV file must have at least a header and one data row for schema inference."
                else do
                    let headers = V.toList $ V.map TE.decodeUtf8 $ records V.! 0
                    let dataRows = V.tail records
                    let columnSamples = V.toList $ V.generate (V.length (V.head records)) $ \colIdx ->
                            V.toList $ V.map (\row -> TE.decodeUtf8 (row V.! colIdx)) dataRows
                    
                    inferredTypes <- sequence $ map inferColumnTypeFromSamples columnSamples
                    let prefixedHeaders = map (T.pack typeName <>) headers
                    let normalizedPrefixedHeaders = map normalizeFieldName prefixedHeaders
                    let schemaListType = foldr (\(h, t) acc -> PromotedConsT `AppT` (PromotedTupleT 2 `AppT` LitT (StrTyLit (T.unpack h)) `AppT` t) `AppT` acc) PromotedNilT (zip prefixedHeaders inferredTypes)
                    let typeSyn = TySynD (mkName typeName) [] schemaListType

                    -- Generate a concrete record type for CSV parsing
                    let recordName = mkName (typeName ++ "Record")
                    recordDec <- dataD (cxt []) recordName [] Nothing 
                                    [recC recordName (zipWith (\h t -> varBangType (mkName (T.unpack h)) (bangType (bang noSourceUnpackedness noSourceStrictness) (pure t))) normalizedPrefixedHeaders inferredTypes)]
                                    [derivClause (Just StockStrategy) [conT ''Show, conT ''Generic], derivClause (Just AnyclassStrategy) [conT ''FromNamedRecord]]

                    -- Generate HasSchema instance
                    hasSchemaInstance <- instanceD (cxt []) [t|HasSchema $(conT recordName)|] [
                        pure $ TySynInstD (TySynEqn Nothing (AppT (ConT ''Schema) (ConT recordName)) schemaListType)
                        ]

                    return [typeSyn, recordDec, hasSchemaInstance]

-- | Reads a CSV file into a `Vector` of records.
-- It normalizes the header fields to be valid Haskell identifiers before decoding.
readCsv :: (FromNamedRecord a) => FilePath -> IO (Either String (V.Vector a))
readCsv filePath = do
    contents <- BL.readFile filePath
    let (headerLine, dataLines) = BL.break (== 10) contents -- 10 is newline character

    -- Decode the header line into individual ByteString fields
    case C.decode NoHeader headerLine of
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
