{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- TestSuite.hs
module Main where

import Control.Exception (throwIO)
import Control.Monad (forM)
import Control.Monad.IO.Class (liftIO)
import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as L
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy(..))
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Time (UTCTime(..), secondsToDiffTime)
import Data.Time.Calendar (fromGregorian)
import qualified Data.Vector as V
import Database.SQLite.Simple
import GHC.TypeLits (Symbol)
import System.IO (hClose)
import System.IO.Temp (withSystemTempFile)
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

-- Sara imports (as used by the original suite)
import Sara.DataFrame.Types
import Sara.DataFrame.Wrangling
  ( selectColumns
  , dropColumns
  , filterRows
  , sortDataFrame
  , SortCriterion(..), SortOrder(..)
  )
import Sara.DataFrame.Transform (applyColumn, mutate)
import Sara.DataFrame.Join (joinDF)
import Sara.DataFrame.Concat (concatDF, ConcatMode(..))
import Sara.DataFrame.Missing (fillna, ffill, bfill, dropna, isna, notna, DropAxis(..))
import Sara.DataFrame.Aggregate
import Sara.DataFrame.Expression
import Sara.DataFrame.SQL (readSQL)
import Sara.DataFrame.IO (readJSONStreaming, writeJSONStreaming, readCsvStreaming)
import Sara.Error (SaraError(..))
import Sara.Schema.Definitions (EmployeesRecord)
import Streaming (Of, Stream)
import qualified Streaming.Prelude as S

---------------------------------------------------------------------------------------------------
-- Small helpers
---------------------------------------------------------------------------------------------------

-- Safer Either un-wrapper for IO-based tests
fromRight' :: Show e => Either e a -> IO a
fromRight' (Right a) = pure a
fromRight' (Left  e) = throwIO (userError ("Expected Right; got Left: " <> show e))

-- Treat "empty" as "no rows"
isEmptyDF :: DataFrame cols -> Bool
isEmptyDF df = null (toRows df)

-- Convert a DataFrame to a stream of single-row DataFrames
dfToStream :: forall cols. KnownColumns cols => DataFrame cols -> Stream (Of (DataFrame cols)) IO ()
dfToStream df = S.each (map (\r -> fromRows @cols [r]) (toRows df))

-- Collect a stream of DataFrames back into one by vertical concatenation
streamToDf :: forall cols. KnownColumns cols => Stream (Of (DataFrame cols)) IO () -> IO (DataFrame cols)
streamToDf s = do
  dfs <- S.toList_ s
  case dfs of
    []   -> pure (fromRows @cols [])
    x:xs -> pure (concatDF (Proxy @cols) ConcatRows (x:xs))

---------------------------------------------------------------------------------------------------
-- Generators for property tests (self-contained; no reliance on Arbitrary instances in the lib)
---------------------------------------------------------------------------------------------------

-- Build a row map quickly
row3 :: (T.Text, DFValue) -> (T.Text, DFValue) -> (T.Text, DFValue) -> Map.Map T.Text DFValue
row3 a b c = Map.fromList [a,b,c]

row2 :: (T.Text, DFValue) -> (T.Text, DFValue) -> Map.Map T.Text DFValue
row2 a b = Map.fromList [a,b]

-- Generators
genName :: Gen T.Text
genName = T.pack <$> listOf (elements (['a'..'z'] ++ ['A'..'Z']))

genCity :: Gen T.Text
genCity = elements (map T.pack ["London","New York","Paris","Delhi","Tokyo","Berlin","Oslo","Seoul"])

genText :: Gen T.Text
genText = T.pack <$> listOf1 (elements (['a'..'z'] ++ " -_"))

genAge :: Gen Int
genAge = chooseInt (0, 100)

genSalary :: Gen Double
genSalary = (/ 10) . fromIntegral <$> chooseInt (0, 2_000_000)

genId :: Gen Int
genId = chooseInt (0, 50)

-- NAs sprinkled in
maybeNA :: DFValue -> Gen DFValue
maybeNA v = frequency [(4, pure v), (1, pure NA)]

-- DataFrames we need repeatedly

-- '[("Name",Text),("Age",Int),("Salary",Double)]
genDF_NameAgeSalary :: Gen (DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])
genDF_NameAgeSalary = do
  n <- chooseInt (0, 25)
  rs <- vectorOf n $ do
          nm <- genName
          ag <- genAge
          sl <- genSalary
          pure $ row3 ("Name", TextValue nm) ("Age", IntValue ag) ("Salary", DoubleValue sl)
  pure (fromRows @'[ '("Name",T.Text), '("Age",Int), '("Salary",Double)] rs)

-- '[("Category",Text),("Value",Int)]
genDF_CategoryValue :: Gen (DataFrame '[ '("Category", T.Text), '("Value", Int)])
genDF_CategoryValue = do
  n <- chooseInt (0, 25)
  rs <- vectorOf n $ do
          cat <- elements (map T.pack ["A","B","C","D"])
          v   <- genAge
          pure $ row2 ("Category", TextValue cat) ("Value", IntValue v)
  pure (fromRows @'[ '("Category",T.Text), '("Value",Int)] rs)

-- join schemas
genDF_ID_Name :: Gen (DataFrame '[ '("ID", Int), '("Name", T.Text)])
genDF_ID_Name = do
  n <- chooseInt (0, 25)
  rs <- vectorOf n $ do
          i  <- genId
          nm <- genName
          pure $ row2 ("ID", IntValue i) ("Name", TextValue nm)
  pure (fromRows @'[ '("ID",Int), '("Name",T.Text)] rs)

genDF_ID_City :: Gen (DataFrame '[ '("ID", Int), '("City", T.Text)])
genDF_ID_City = do
  n <- chooseInt (0, 25)
  rs <- vectorOf n $ do
          i  <- genId
          c  <- genCity
          pure $ row2 ("ID", IntValue i) ("City", TextValue c)
  pure (fromRows @'[ '("ID",Int), '("City",T.Text)] rs)

-- Newtype wrappers so QuickCheck can derive Arbitrary
newtype DF_NAS     = DF_NAS (DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)]) deriving Show
newtype DF_CatVal  = DF_CatVal (DataFrame '[ '("Category", T.Text), '("Value", Int)]) deriving Show
newtype DF_IDName  = DF_IDName (DataFrame '[ '("ID", Int), '("Name", T.Text)]) deriving Show
newtype DF_IDCity  = DF_IDCity (DataFrame '[ '("ID", Int), '("City", T.Text)]) deriving Show

instance Arbitrary DF_NAS    where arbitrary = DF_NAS    <$> genDF_NameAgeSalary
instance Arbitrary DF_CatVal where arbitrary = DF_CatVal <$> genDF_CategoryValue
instance Arbitrary DF_IDName where arbitrary = DF_IDName <$> genDF_ID_Name
instance Arbitrary DF_IDCity where arbitrary = DF_IDCity <$> genDF_ID_City

---------------------------------------------------------------------------------------------------
-- Unit Tests + Properties
---------------------------------------------------------------------------------------------------

main :: IO ()
main = hspec $ do
  -----------------------------------------------------------------------------------------------
  -- Basic Expression & Column semantics
  -----------------------------------------------------------------------------------------------
  describe "Expression evaluation & type-safe col" $ do
    it "col picks the right value at a row index" $ do
      let df = fromRows @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)]
                 [ Map.fromList [("Name", TextValue "Alice"), ("Age", IntValue 30), ("Salary", DoubleValue 50000.0)] ]
          ageE :: Expr '[ '("Name",T.Text), '("Age",Int), '("Salary",Double)] Int
          ageE = col (Proxy @"Age")
      evaluateExpr ageE (getDataFrameMap df) 0 `shouldBe` Right 30

    it "literal arithmetic works" $ do
      let dm = Map.empty :: Map.Map T.Text (V.Vector DFValue)
      evaluateExpr (lit (5::Int) +.+ lit (3::Int)) dm 0 `shouldBe` Right 8
      evaluateExpr (lit (10::Double) /.! lit (2::Double)) dm 0 `shouldBe` Right 5.0

  -----------------------------------------------------------------------------------------------
  -- filterRows / applyColumn / sort / mutate
  -----------------------------------------------------------------------------------------------
  describe "Wrangling primitives" $ do
    it "filterRows keeps rows that satisfy predicate" $ do
      let df = fromRows @'[ '("Name",T.Text), '("Age",Int), '("Salary",Double)]
                 [ Map.fromList [("Name",TextValue "A"),("Age",IntValue 20),("Salary",DoubleValue 1.0)]
                 , Map.fromList [("Name",TextValue "B"),("Age",IntValue 31),("Salary",DoubleValue 2.0)]
                 , Map.fromList [("Name",TextValue "C"),("Age",IntValue 40),("Salary",DoubleValue 3.0)]
                 ]
          p  = (col (Proxy @"Age") :: Expr _ Int) >. lit (30 :: Int)
      filtered <- streamToDf (filterRows p (dfToStream df))
      length (toRows filtered) `shouldBe` 2

    it "applyColumn modifies just one column" $ do
      let df = fromRows @'[ '("Name",T.Text), '("Age",Int), '("Salary",Double)]
                 [ Map.fromList [("Name",TextValue "A"),("Age",IntValue 5),("Salary",DoubleValue 0.0)] ]
      out <- streamToDf (applyColumn (Proxy @"Age") ((+2) :: Int -> Int) (dfToStream df))
      let [r] = toRows out
      fromDFValue @Int (r Map.! "Age") `shouldBe` Right 7
      fromDFValue @T.Text (r Map.! "Name") `shouldBe` Right "A"

    it "sortDataFrame sorts by a single criterion" $ do
      let df = fromRows @'[ '("Name",T.Text), '("Age",Int), '("Salary",Double)]
                 [ Map.fromList [("Name",TextValue "C"),("Age",IntValue 30),("Salary",DoubleValue 1)]
                 , Map.fromList [("Name",TextValue "A"),("Age",IntValue 10),("Salary",DoubleValue 2)]
                 , Map.fromList [("Name",TextValue "B"),("Age",IntValue 20),("Salary",DoubleValue 3)]
                 ]
          sorted = sortDataFrame [SortCriterion (Proxy @"Name") Ascending] df
      mapM (fromRight' . fromDFValue @T.Text . (Map.! "Name")) (toRows sorted)
        `shouldReturn` ["A","B","C"]

    it "mutate adds a new computable column" $ do
      let df = fromRows @'[ '("Name",T.Text), '("Age",Int), '("Salary",Double)]
                 [ Map.fromList [("Name",TextValue "A"),("Age",IntValue 5),("Salary",DoubleValue 0.0)] ]
          expr = (col (Proxy @"Age") :: Expr _ Int) +.+ lit (5::Int)
      case mutate @"AgePlusFive" (Proxy @"AgePlusFive") expr df of
        Left e   -> expectationFailure (show e)
        Right df' -> do
          let [r] = toRows df'
          fromDFValue @Int (r Map.! "AgePlusFive") `shouldBe` Right 10

  -----------------------------------------------------------------------------------------------
  -- Join
  -----------------------------------------------------------------------------------------------
  describe "joinDF" $ do
    it "performs an inner join on ID" $ do
      let left  = fromRows @'[ '("ID",Int), '("Name",T.Text)]
                    [ Map.fromList [("ID",IntValue 1),("Name",TextValue "Alice")]
                    , Map.fromList [("ID",IntValue 2),("Name",TextValue "Bob")]
                    ]
          right = fromRows @'[ '("ID",Int), '("City",T.Text)]
                    [ Map.fromList [("ID",IntValue 1),("City",TextValue "New York")]
                    , Map.fromList [("ID",IntValue 3),("City",TextValue "London")]
                    ]
          joined = joinDF @'["ID"] left right
      length (toRows joined) `shouldBe` 1
      let [r] = toRows joined
      fromDFValue @Int     (r Map.! "ID")   `shouldBe` Right 1
      fromDFValue @T.Text  (r Map.! "Name") `shouldBe` Right "Alice"
      fromDFValue @T.Text  (r Map.! "City") `shouldBe` Right "New York"

  -----------------------------------------------------------------------------------------------
  -- selectColumns / dropColumns
  -----------------------------------------------------------------------------------------------
  describe "selectColumns & dropColumns" $ do
    it "selectColumns keeps only requested columns" $ do
      let df = fromRows @'[ '("Name",T.Text), '("Age",Int), '("Salary",Double)]
               [ Map.fromList [("Name",TextValue "A"),("Age",IntValue 1),("Salary",DoubleValue 1.0)] ]
          df' = selectColumns @'["Name","Salary"] df
          [r] = toRows df'
      Map.keys r `shouldMatchList` ["Name","Salary"]

    it "dropColumns removes requested columns" $ do
      let df = fromRows @'[ '("Name",T.Text), '("Age",Int), '("Salary",Double)]
               [ Map.fromList [("Name",TextValue "A"),("Age",IntValue 1),("Salary",DoubleValue 1.0)] ]
          df' = dropColumns @'["Age"] df
          [r] = toRows df'
      Map.keys r `shouldMatchList` ["Name","Salary"]

  -----------------------------------------------------------------------------------------------
  -- Aggregations
  -----------------------------------------------------------------------------------------------
  describe "Aggregations (sum / mean / count)" $ do
    it "sumAgg by Category" $ do
      let df = fromRows @'[ '("Category",T.Text), '("Value",Int)]
               [ row2 ("Category",TextValue "A") ("Value",IntValue 10)
               , row2 ("Category",TextValue "A") ("Value",IntValue 20)
               , row2 ("Category",TextValue "B") ("Value",IntValue 30)
               ]
      gs <- pure =<< groupBy @'["Category"] (dfToStream df)
      out <- sumAgg @"Value_sum" @"Value" (Proxy @"Value_sum") (pure gs)
      let m = Map.fromList . map (\r -> ( expect @T.Text r "Category"
                                        , expect @Double r "Value_sum")) $ toRows out
      m Map.! "A" `shouldBe` 30.0
      m Map.! "B" `shouldBe` 30.0

    it "meanAgg by Category" $ do
      let df = fromRows @'[ '("Category",T.Text), '("Value",Int)]
               [ row2 ("Category",TextValue "A") ("Value",IntValue 10)
               , row2 ("Category",TextValue "A") ("Value",IntValue 20)
               , row2 ("Category",TextValue "B") ("Value",IntValue 30)
               , row2 ("Category",TextValue "B") ("Value",IntValue 50)
               ]
      gs <- pure =<< groupBy @'["Category"] (dfToStream df)
      out <- meanAgg @"Value_mean" @"Value" (Proxy @"Value_mean") (pure gs)
      let m = Map.fromList . map (\r -> ( expect @T.Text r "Category"
                                        , expect @Double r "Value_mean")) $ toRows out
      m Map.! "A" `shouldBe` 15.0
      m Map.! "B" `shouldBe` 40.0

    it "countAgg by Category" $ do
      let df = fromRows @'[ '("Category",T.Text), '("Value",Int)]
               [ row2 ("Category",TextValue "A") ("Value",IntValue 10)
               , row2 ("Category",TextValue "A") ("Value",IntValue 20)
               , row2 ("Category",TextValue "B") ("Value",IntValue 30)
               ]
      gs <- pure =<< groupBy @'["Category"] (dfToStream df)
      out <- countAgg @"Category_count" @"Category" (Proxy @"Category_count") (pure gs)
      let m = Map.fromList . map (\r -> ( expect @T.Text r "Category"
                                        , expect @Int r "Category_count")) $ toRows out
      m Map.! "A" `shouldBe` 2
      m Map.! "B" `shouldBe` 1

  -----------------------------------------------------------------------------------------------
  -- Concatenation
  -----------------------------------------------------------------------------------------------
  describe "concatDF" $ do
    it "ConcatRows appends rows" $ do
      let df1 = fromRows @'[ '("A",Int), '("B",T.Text)]
                [ Map.fromList [("A",IntValue 1),("B",TextValue "x")] ]
          df2 = fromRows @'[ '("A",Int), '("B",T.Text)]
                [ Map.fromList [("A",IntValue 2),("B",TextValue "y")] ]
          out = concatDF (Proxy @'[ '("A",Int), '("B",T.Text)]) ConcatRows [df1,df2]
      mapM (fromRight' . fromDFValue @Int . (Map.! "A")) (toRows out)
        `shouldReturn` [1,2]

    it "ConcatColumns (last one wins on overlap)" $ do
      let a = fromRows @'[ '("A",Int), '("B",T.Text)]
                [ Map.fromList [("A",IntValue 1),("B",TextValue "x")] ]
          b = fromRows @'[ '("A",Int), '("B",T.Text)]
                [ Map.fromList [("A",IntValue 2),("B",TextValue "y")] ]
          out = concatDF (Proxy @'[ '("A",Int), '("B",T.Text)]) ConcatColumns [a,b]
      toRows out `shouldBe` [ Map.fromList [("A",IntValue 2),("B",TextValue "y")] ]

  -----------------------------------------------------------------------------------------------
  -- Missing-data utilities
  -----------------------------------------------------------------------------------------------
  describe "Missing-data helpers" $ do
    let dfNA = fromRows @'[ '("colA",Int), '("colB",T.Text), '("colC",Double)]
                 [ Map.fromList [("colA",IntValue 1), ("colB",TextValue "a"), ("colC",DoubleValue 1.1)]
                 , Map.fromList [("colA",NA),        ("colB",TextValue "b"), ("colC",NA)]
                 , Map.fromList [("colA",IntValue 3), ("colB",NA),           ("colC",DoubleValue 3.3)]
                 , Map.fromList [("colA",NA),        ("colB",NA),            ("colC",NA)]
                 ]

    it "fillna (single column)" $ do
      let out = fillna dfNA (Proxy @Int) (Just "colA") (99 :: Int)
          expected = fromRows @'[ '("colA",Int), '("colB",T.Text), '("colC",Double)]
                       [ Map.fromList [("colA",IntValue 1),  ("colB",TextValue "a"), ("colC",DoubleValue 1.1)]
                       , Map.fromList [("colA",IntValue 99), ("colB",TextValue "b"), ("colC",NA)]
                       , Map.fromList [("colA",IntValue 3),  ("colB",NA),            ("colC",DoubleValue 3.3)]
                       , Map.fromList [("colA",IntValue 99), ("colB",NA),            ("colC",NA)]
                       ]
      toRows out `shouldBe` toRows expected

    it "ffill" $ do
      let d = fromRows @'[ '("colA",Int)]
                [ Map.fromList [("colA",IntValue 1)]
                , Map.fromList [("colA",NA)]
                , Map.fromList [("colA",IntValue 3)]
                , Map.fromList [("colA",NA)]
                ]
          e = fromRows @'[ '("colA",Int)]
                [ Map.fromList [("colA",IntValue 1)]
                , Map.fromList [("colA",IntValue 1)]
                , Map.fromList [("colA",IntValue 3)]
                , Map.fromList [("colA",IntValue 3)]
                ]
      toRows (ffill d) `shouldBe` toRows e

    it "bfill" $ do
      let d = fromRows @'[ '("colA",Int)]
                [ Map.fromList [("colA",NA)]
                , Map.fromList [("colA",IntValue 2)]
                , Map.fromList [("colA",NA)]
                , Map.fromList [("colA",IntValue 4)]
                ]
          e = fromRows @'[ '("colA",Int)]
                [ Map.fromList [("colA",IntValue 2)]
                , Map.fromList [("colA",IntValue 2)]
                , Map.fromList [("colA",IntValue 4)]
                , Map.fromList [("colA",IntValue 4)]
                ]
      toRows (bfill d) `shouldBe` toRows e

    it "dropna rows (any NA)" $ do
      let out = dropna dfNA (DropAxis DropRows) Nothing
      length (toRows out) `shouldBe` 1

    it "isna / notna shapes are preserved" $ do
      let s1 = toRows (isna dfNA)
          s2 = toRows (notna dfNA)
      length s1 `shouldBe` length (toRows dfNA)
      length s2 `shouldBe` length (toRows dfNA)

  -----------------------------------------------------------------------------------------------
  -- JSON & CSV streaming (robust error expectations)
  -----------------------------------------------------------------------------------------------
  describe "JSON/CSV streaming IO" $ do
    it "roundtrips JSON (streaming)" $ do
      let df = fromRows @'[ '("name",T.Text), '("age",Int)]
                [ Map.fromList [("name",TextValue "Alice"),("age",IntValue 30)]
                , Map.fromList [("name",TextValue "Bob"),  ("age",IntValue 25)]
                ]
      withSystemTempFile "rt.json" $ \fp h -> do
        BL.hPutStr h "" >> hClose h
        writeJSONStreaming fp (dfToStream df)
        r <- readJSONStreaming (Proxy @'[ '("name",T.Text), '("age",Int)]) fp
        s <- either (const (expectationFailure "readJSONStreaming failed") >> pure (fromRows @'[] []))
                    streamToDf
                    r
        s `shouldBe` df

    it "non-existent JSON file yields IOError" $ do
      r <- readJSONStreaming (Proxy @'[ '("name",T.Text), '("age",Int)]) "does-not-exist.json"
      case r of
        Left (IOError e:_) -> e `shouldSatisfy` T.isInfixOf "does-not-exist.json"
        _ -> expectationFailure "Expected IOError"

    it "malformed JSON reports parsing error" $ do
      withSystemTempFile "bad.json" $ \fp h -> do
        BL.hPutStr h "[{" >> hClose h
        r <- readJSONStreaming (Proxy @'[ '("name",T.Text), '("age",Int)]) fp
        case r of
          Left (ParsingError e:_) -> e `shouldSatisfy` (not . T.null)
          _ -> expectationFailure "Expected ParsingError"

    it "empty CSV produces parse error" $ do
      withSystemTempFile "empty.csv" $ \fp h -> do
        BL.hPutStr h "" >> hClose h
        r <- readCsvStreaming (Proxy @EmployeesRecord) fp
        case r of
          Left es -> es `shouldSatisfy` (not . null)
          _       -> expectationFailure "Expected Left for empty CSV"

    it "non-existent CSV gives IOError" $ do
      r <- readCsvStreaming (Proxy @EmployeesRecord) "nope.csv"
      case r of
        Left (IOError e:_) -> e `shouldSatisfy` T.isInfixOf "nope.csv"
        _ -> expectationFailure "Expected IOError"

  -----------------------------------------------------------------------------------------------
  -- SQL integration (kept lightweight & robust)
  -----------------------------------------------------------------------------------------------
  describe "SQL integration" $ do
    let withTempDb action =
          withSystemTempFile "test.db" $ \fp h -> hClose h >> action fp

    it "reads a simple table" $ do
      withTempDb $ \dbPath -> do
        conn <- open dbPath
        execute_ conn "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
        execute_ conn "INSERT INTO users (id, name, age) VALUES (1,'Alice',30)"
        execute_ conn "INSERT INTO users (id, name, age) VALUES (2,'Bob',25)"
        close conn
        r <- readSQL (Proxy @'[ '("id",Int), '("name",T.Text), '("age",Int)]) dbPath
                      "SELECT id, name, age FROM users ORDER BY id"
        df <- fromRight' r
        mapM (fromRight' . fromDFValue @Int . (Map.! "id")) (toRows df)
          `shouldReturn` [1,2]

    it "handles missing table gracefully" $ do
      let q = "SELECT id FROM not_a_table"
      r <- readSQL (Proxy @'[ '("id",Int)]) "brand-new.db" q
      case r of
        Left (GenericError e) -> e `shouldSatisfy` T.isInfixOf "no such table"
        _                     -> expectationFailure "Expected GenericError"

  -----------------------------------------------------------------------------------------------
  -- DFValue roundtrips & introspection
  -----------------------------------------------------------------------------------------------
  describe "DFValue JSON & type inspection" $ do
    it "DFValue JSON roundtrips for basic types" $ do
      let xs = [ IntValue 1
               , DoubleValue 2.5
               , TextValue "x"
               , BoolValue True
               , DateValue (fromGregorian 2024 1 1)
               , TimestampValue (UTCTime (fromGregorian 2024 1 1) (secondsToDiffTime 60))
               , NA
               ]
      map A.decode (map A.encode xs) `shouldBe` map Just xs

    it "toDFValue/fromDFValue roundtrips" $ do
      fromDFValue @Int    (toDFValue (123::Int))        `shouldBe` Right 123
      fromDFValue @Double (toDFValue (1.25::Double))    `shouldBe` Right 1.25
      fromDFValue @T.Text (toDFValue ("hi"::T.Text))    `shouldBe` Right "hi"
      fromDFValue @Bool   (toDFValue True)              `shouldBe` Right True
      (fromDFValue @(Maybe Int) (toDFValue (Nothing::Maybe Int))) `shouldBe` Right Nothing
      (fromDFValue @(Maybe Int) (toDFValue (Just 7)))             `shouldBe` Right (Just 7)

    it "getDFValueType reports expected TypeRep (or Nothing for NA)" $ do
      getDFValueType (IntValue  1) `shouldBe` Just (typeRep (Proxy @Int))
      getDFValueType (DoubleValue 1) `shouldBe` Just (typeRep (Proxy @Double))
      getDFValueType (TextValue "a") `shouldBe` Just (typeRep (Proxy @T.Text))
      getDFValueType (BoolValue True) `shouldBe` Just (typeRep (Proxy @Bool))
      getDFValueType NA `shouldBe` Nothing

  -----------------------------------------------------------------------------------------------
  -- QuickCheck Properties (randomized, yet tight & robust)
  -----------------------------------------------------------------------------------------------
  describe "QuickCheck properties" $ do
    prop "fromRows . toRows = id (Name/Age/Salary)" $
      \(DF_NAS df) -> (fromRows . toRows) df === df

    prop "filterRows True ≡ identity" $
      \(DF_NAS df) -> ioProperty $ do
        out <- streamToDf (filterRows (lit True) (dfToStream df))
        pure (out === df)

    prop "filterRows False produces empty DF" $
      \(DF_NAS df) -> ioProperty $ do
        out <- streamToDf (filterRows (lit False) (dfToStream df))
        pure (toRows out === [])

    prop "selectColumns preserves selected values" $
      \(DF_NAS df) ->
        let sel = selectColumns @'["Name","Age"] df
        in  map (\r -> (Map.lookup "Name" r, Map.lookup "Age" r)) (toRows sel)
            ===
            map (\r -> (Map.lookup "Name" r, Map.lookup "Age" r)) (toRows df)

    prop "dropColumns keeps remaining values intact" $
      \(DF_NAS df) ->
        let dr = dropColumns @'["Age"] df
        in  map (Map.lookup "Name" &&& Map.lookup "Salary") (toRows dr)
            ===
            map (Map.lookup "Name" &&& Map.lookup "Salary") (toRows df)

    prop "sortDataFrame by Name is stable w.r.t. multiset of rows" $
      \(DF_NAS df) ->
        let sorted = sortDataFrame [SortCriterion (Proxy @"Name") Ascending] df
            bag xs = L.sort xs
        in  bag (toRows sorted) === bag (toRows df)

    prop "applyColumn (+1) on Age only affects Age" $
      \(DF_NAS df) -> ioProperty $ do
        out <- streamToDf (applyColumn (Proxy @"Age") ((+1) :: Int -> Int) (dfToStream df))
        let origAges = map (fromRight' . fromDFValue @Int . (Map.! "Age")) (toRows df)
        let outAges  = map (fromRight' . fromDFValue @Int . (Map.! "Age")) (toRows out)
        oa  <- sequence origAges
        ob  <- sequence outAges
        let inc = map (+1) oa
        pure (ob === inc)

    prop "mutate adds AgePlusFive = Age + 5" $
      \(DF_NAS df) ->
        case mutate @"AgePlusFive" (Proxy @"AgePlusFive")
                    ((col (Proxy @"Age") :: Expr _ Int) +.+ lit (5::Int)) df of
          Left _  -> property False
          Right d ->
            let expect' r = do
                  a  <- fromDFValue @Int (r Map.! "Age")
                  ap <- fromDFValue @Int (r Map.! "AgePlusFive")
                  pure (ap == a + 5)
            in  conjoin (map (property . unsafeRightIO . expect') (toRows d))

    prop "sumAgg per Category equals manual totals" $
      \(DF_CatVal df) -> ioProperty $ do
        gs  <- groupBy @'["Category"] (dfToStream df)
        out <- sumAgg @"Value_sum" @"Value" (Proxy @"Value_sum") (pure gs)
        -- expected
        let sums = Map.fromListWith (+)
                   [ (unsafeRight (fromDFValue @T.Text (r Map.! "Category"))
                     , fromIntegral (unsafeRight (fromDFValue @Int (r Map.! "Value")) :: Int))
                   | r <- toRows df
                   ]
        -- actual
        let sums' = Map.fromList
                    [ ( unsafeRight (fromDFValue @T.Text (r Map.! "Category"))
                      , unsafeRight (fromDFValue @Double (r Map.! "Value_sum")) )
                    | r <- toRows out
                    ]
        pure (sums' === sums)

    prop "meanAgg per Category equals manual means" $
      \(DF_CatVal df) -> ioProperty $ do
        gs  <- groupBy @'["Category"] (dfToStream df)
        out <- meanAgg @"Value_mean" @"Value" (Proxy @"Value_mean") (pure gs)
        let groups = Map.fromListWith (++)
                     [ ( unsafeRight (fromDFValue @T.Text (r Map.! "Category"))
                       , [fromIntegral (unsafeRight (fromDFValue @Int (r Map.! "Value")) :: Int)])
                     | r <- toRows df
                     ]
            means = Map.map (\xs -> sum xs / fromIntegral (length xs)) groups
        let means' = Map.fromList
                     [ ( unsafeRight (fromDFValue @T.Text (r Map.! "Category"))
                       , unsafeRight (fromDFValue @Double (r Map.! "Value_mean")) )
                     | r <- toRows out
                     ]
        pure (means' === means)

    prop "countAgg per Category equals manual counts" $
      \(DF_CatVal df) -> ioProperty $ do
        gs  <- groupBy @'["Category"] (dfToStream df)
        out <- countAgg @"Category_count" @"Category" (Proxy @"Category_count") (pure gs)
        let counts  = Map.fromListWith (+)
                      [ ( unsafeRight (fromDFValue @T.Text (r Map.! "Category")), 1 :: Int)
                      | r <- toRows df
                      ]
        let counts' = Map.fromList
                      [ ( unsafeRight (fromDFValue @T.Text (r Map.! "Category"))
                        , unsafeRight (fromDFValue @Int (r Map.! "Category_count")) )
                      | r <- toRows out
                      ]
        pure (counts' === counts)

    prop "joinDF on ID yields intersection of IDs" $
      \(DF_IDName l) (DF_IDCity r) ->
        let j = joinDF @'["ID"] l r
            idsJ = Set.fromList $ map (unsafeRight . fromDFValue @Int . (Map.! "ID")) (toRows j)
            idsL = Set.fromList $ map (unsafeRight . fromDFValue @Int . (Map.! "ID")) (toRows l)
            idsR = Set.fromList $ map (unsafeRight . fromDFValue @Int . (Map.! "ID")) (toRows r)
        in  idsJ === Set.intersection idsL idsR

---------------------------------------------------------------------------------------------------
-- Tiny utilities for expectations in maps/rows
---------------------------------------------------------------------------------------------------

-- fetch and decode a typed DFValue from a row; crash the test meaningfully if missing
expect :: forall a. CanBeDFValue a => Map.Map T.Text DFValue -> T.Text -> a
expect r k = case Map.lookup k r of
  Nothing -> error ("Missing key " <> T.unpack k)
  Just v  -> case fromDFValue @a v of
               Left  e -> error ("Type error for " <> T.unpack k <> ": " <> show e)
               Right x -> x

(&&&) :: (a -> b) -> (a -> c) -> a -> (b,c)
(&&&) f g x = (f x, g x)

-- Convert Either in pure-properties where we *know* success due to how we build data
unsafeRight :: Show e => Either e a -> a
unsafeRight (Right a) = a
unsafeRight (Left e)  = error ("Unexpected Left: " <> show e)

-- Same but inside 'property' contexts that use IO earlier
unsafeRightIO :: Show e => Either e a -> a
unsafeRightIO = unsafeRight

