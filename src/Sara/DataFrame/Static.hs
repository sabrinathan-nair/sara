-- | This module provides Template Haskell functions for statically inferring schemas
-- from CSV files. This allows for creating `DataFrame`s with compile-time guarantees
-- about column names and types.
{-# LANGUAGE TemplateHaskell #-}

module Sara.DataFrame.Static ( 
    -- * Template Haskell Functions
    tableTypes,
    inferCsvSchema,
    -- * CSV Reading
    readCsv,
    -- * Testable helpers (exported for tests)
    collectColumnSamplesText
) where

import Language.Haskell.TH
import Data.Csv (FromNamedRecord, HasHeader(NoHeader))
import qualified Data.ByteString.Lazy as BL
import qualified Data.Vector as V
import Data.Char (toLower)
import GHC.Generics (Generic)
import qualified Data.ByteString.Char8 as BC
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Csv as C
import Data.Time (Day, UTCTime)
import Data.Time.Format (parseTimeM, defaultTimeLocale)
import Text.Read (readEither)
import Data.Maybe (isJust, fromMaybe)
import Data.Either (isRight)
import Data.Bifunctor (first)
import qualified Data.HashMap.Strict as HM

-- Local modules
import Sara.DataFrame.Internal (HasSchema(..), HasTypeName(..))
import Sara.Error (SaraError(..))
import Sara.Internal.Safe (safeHead)

--------------------------------------------------------------------------------
-- Field name normalization
--------------------------------------------------------------------------------

normalizeFieldName :: Text -> Text
normalizeFieldName t = case T.uncons (T.replace (T.pack ".") (T.pack "_") t) of
    Just (x, xs) -> T.cons (toLower x) xs
    Nothing      -> T.empty


--------------------------------------------------------------------------------
-- Type inference
--------------------------------------------------------------------------------

inferColumnTypeFromSamples :: [Text] -> Q Type
inferColumnTypeFromSamples samples = do
    let nonNaSamples = filter (\s -> T.toLower s /= T.pack "na" && not (T.null s)) samples
        hasNa        = length nonNaSamples < length samples

    baseTypeQ <- if null nonNaSamples
        then [t| Text |]
        else inferMostSpecificType nonNaSamples

    if hasNa
        then [t| Maybe $(pure baseTypeQ) |]
        else pure baseTypeQ

inferMostSpecificType :: [Text] -> Q Type
inferMostSpecificType []    = [t| Text |]
inferMostSpecificType (s:_) = inferDFType s

inferDFType :: Text -> Q Type
inferDFType s = do
  let s_unpack = T.unpack s
  if isRight (first (const [ParsingError (T.pack "Invalid Int")]) (readEither s_unpack) :: Either [SaraError] Int)
     then [t| Int |]
  else if isRight (first (const [ParsingError (T.pack "Invalid Double")]) (readEither s_unpack) :: Either [SaraError] Double)
     then [t| Double |]
  else if isJust (parseTimeM True defaultTimeLocale "%Y-%m-%d" s_unpack :: Maybe Day)
     then [t| Day |]
  else if isJust (parseTimeM True defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" s_unpack :: Maybe UTCTime)
     then [t| UTCTime |]
  else if let ls = T.toLower s in ls == T.pack "true" || ls == T.pack "false"
     then [t| Bool |]
  else [t| Text |]

--------------------------------------------------------------------------------
-- Safe column sampling from rows
--------------------------------------------------------------------------------

collectColumnSamplesText
  :: V.Vector BC.ByteString        -- ^ header row (bytes)
  -> [V.Vector BC.ByteString]      -- ^ data rows (bytes)
  -> [[Text]]                      -- ^ column-wise samples (as Text)
collectColumnSamplesText headerRow dataRows =
  let numCols = V.length headerRow
      atOrEmpty v i = fromMaybe BC.empty (v V.!? i)
      column j = [ TE.decodeUtf8 (atOrEmpty row j) | row <- dataRows ]
  in [ column j | j <- [0 .. numCols - 1] ]

--------------------------------------------------------------------------------
-- Template Haskell: tableTypes
--------------------------------------------------------------------------------

tableTypes :: String -> FilePath -> Q [Dec]
tableTypes name filePath = do
    contents <- runIO $ BL.readFile filePath
    case C.decode C.NoHeader contents of
        Left err -> fail err
        Right records -> 
            case V.toList records of
                [] -> fail "CSV file is empty."
                (headerRow:dataRows) ->
                    case safeHead dataRows of
                        Nothing -> fail "CSV file must have at least one data row."
                        Just firstRowVec -> do
                            let headers  = V.toList $ V.map TE.decodeUtf8 headerRow
                                firstRow = V.toList $ V.map TE.decodeUtf8 firstRowVec
                            fields <- mapM
                              (\(h,t) -> do
                                  ty <- inferDFType t
                                  pure (mkName (T.unpack (normalizeFieldName h)), ty)
                              )
                              (zip headers firstRow)
                            let recordName = mkName name
                            record <- dataD (cxt []) recordName [] Nothing
                                      [ recC recordName
                                          ( map (\(n, t) ->
                                               varBangType n
                                                 (bangType
                                                   (bang noSourceUnpackedness noSourceStrictness)
                                                   (pure t)))
                                                fields
                                          )
                                      ]
                                      [ derivClause (Just StockStrategy)
                                          [ [t| Show |], [t| Generic |] ]
                                      ]
                            pure [record]


--------------------------------------------------------------------------------
-- Template Haskell: inferCsvSchema
--------------------------------------------------------------------------------

inferCsvSchema :: String -> Bool -> FilePath -> Q [Dec]
inferCsvSchema typeName withPrefix filePath = do
    contents <- runIO $ BL.readFile filePath
    case C.decode NoHeader contents :: Either String (V.Vector (V.Vector BC.ByteString)) of
        Left err -> fail . show $ ParsingError (T.pack err)
        Right records ->
            case V.toList records of
                [] -> fail "CSV file is empty."
                (headerRow:dataRows) -> do
                    if null dataRows
                        then fail "CSV file must have at least one data row for schema inference."
                        else do
                            let headersBS   = V.toList headerRow
                                headersTxt  = map TE.decodeUtf8 headersBS
                                columnSamples = collectColumnSamplesText headerRow dataRows

                            inferredTypes <- mapM inferColumnTypeFromSamples columnSamples

                            let finalHeaders =
                                  if withPrefix
                                      then map (\h -> T.pack typeName <> T.pack "." <> h) headersTxt
                                      else headersTxt

                                normalizedFinalHeaders = map normalizeFieldName finalHeaders

                                schemaListType =
                                  foldr
                                    (\(h, t) acc ->
                                       PromotedConsT
                                         `AppT`
                                           (PromotedTupleT 2
                                              `AppT` LitT (StrTyLit (T.unpack h))
                                              `AppT` t)
                                         `AppT` acc)
                                    PromotedNilT
                                    (zip finalHeaders inferredTypes)

                                typeSyn = TySynD (mkName typeName) [] schemaListType
                                recordName = mkName (typeName ++ "Record")

                            recordDec <- dataD (cxt []) recordName [] Nothing 
                                           [ recC recordName
                                               ( zipWith
                                                   (\h t ->
                                                      varBangType (mkName (T.unpack h))
                                                        (bangType (bang noSourceUnpackedness noSourceStrictness) (pure t)))
                                                   normalizedFinalHeaders
                                                   inferredTypes
                                               )
                                           ]
                                           [ derivClause (Just StockStrategy)
                                               [ [t| Show |], [t| Generic |] ]
                                           ]

                            -- FromNamedRecord instance using generic parser
                            fromNamedRecordInstance <-
                              instanceD (cxt [])
                                [t| FromNamedRecord $(conT recordName) |]
                                [ funD 'C.parseNamedRecord
                                    [ clause []
                                        (normalB [| C.genericParseNamedRecord C.defaultOptions |])
                                        []
                                    ]
                                ]

                            -- HasSchema instance with associated type
                            hasSchemaInstance <-
                              instanceD (cxt [])
                                [t| HasSchema $(conT recordName) |]
                                [ tySynInstD (tySynEqn Nothing
                                    [t| Schema $(conT recordName) |]
                                    (pure schemaListType)
                                  )
                                ]

                            -- HasTypeName instance
                            hasTypeNameInstance <-
                              instanceD (cxt [])
                                [t| HasTypeName $(conT recordName) |]
                                [ funD 'getTypeName
                                    [ clause [wildP]
                                        (normalB (litE (stringL typeName)))
                                        []
                                    ]
                                ]

                            pure [ typeSyn
                                 , recordDec
                                 , fromNamedRecordInstance
                                 , hasSchemaInstance
                                 , hasTypeNameInstance
                                 ]


--------------------------------------------------------------------------------
-- CSV reading
--------------------------------------------------------------------------------

readCsv :: (FromNamedRecord a) => FilePath -> IO (Either [SaraError] (V.Vector a))
readCsv fp = do
  contents <- BL.readFile fp
  case C.decode C.NoHeader contents :: Either String (V.Vector (V.Vector BC.ByteString)) of
    Left err -> pure $ Left [ParsingError (T.pack err)]
    Right rows ->
      if V.null rows
        then pure $ Left [ParsingError (T.pack "CSV file is empty.")]
        else do
          let headerRow = V.head rows
              dataRows  = V.toList (V.tail rows)
              headersTxt = map (normalizeFieldName . TE.decodeUtf8) (V.toList headerRow)
              numCols    = V.length headerRow

              mkNamed row =
                let vals = [ fromMaybe BC.empty (row V.!? j) | j <- [0 .. numCols - 1] ]
                    keys = map TE.encodeUtf8 headersTxt
                in HM.fromList (zip keys vals)

              parseRow r = C.runParser (C.parseNamedRecord (mkNamed r))

          pure $ first (pure . ParsingError . T.pack) (traverse parseRow (V.fromList dataRows))
