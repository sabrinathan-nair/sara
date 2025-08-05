{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Main where

import Test.Hspec
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.Transform (mutate, applyColumn)
import Sara.DataFrame.Wrangling (selectColumns, SelectCols, AllKnownSymbol, dropColumns, DropColumns, filterRows, sortDataFrame, symbolsToTexts, MapFst)
import Sara.DataFrame.Types
import Sara.DataFrame.Aggregate
import Sara.DataFrame.Expression
import Sara.DataFrame.Predicate hiding ((===))
import Sara.DataFrame.Join (joinDF, CreateOutputRow)
import Sara.DataFrame.Concat (concatDF)
import Sara.DataFrame.Missing (fillna, ffill, bfill, dropna, isna, notna, DropAxis(..))
import Data.Proxy (Proxy(..))
import Streaming (Stream, Of)
import qualified Streaming.Prelude as S
import Sara.DataFrame.IO (readJSONStreaming, writeJSONStreaming, readCsvStreaming)
import Sara.DataFrame.SQL (readSQL)
import Database.SQLite.Simple
import Sara.Schema.Definitions (EmployeesRecord)
import Data.Either (partitionEithers)
import Control.Monad.IO.Class (liftIO)
import Sara.Error (SaraError(..))
import System.IO.Temp (withSystemTempFile)
import qualified Data.ByteString.Lazy as BL
import System.IO (hClose)
import Test.QuickCheck
import Test.Hspec.QuickCheck
import GHC.TypeLits (Symbol, symbolVal)
import Data.List (sortBy)
import qualified Data.List as L
import Data.Typeable (TypeRep, typeRep)
import qualified Data.Set as Set
import Data.Time (Day(..), UTCTime(..))
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (secondsToDiffTime)
import qualified Data.Aeson as A

-- Helper function to extract value from Either in tests
fromRight' :: (Show e) => Either e a -> a
fromRight' (Right a) = a
fromRight' (Left e) = error ("Test failed: expected Right, got Left: " ++ show e)

-- Helper function to convert a DataFrame to a Stream of single-row DataFrames
dfToStream :: DataFrame cols -> Stream (Of (DataFrame cols)) IO ()
dfToStream df = S.each (map (\row -> DataFrame (Map.map V.singleton row)) (toRows df))

-- Helper function to combine two DataFrames (pure version)
combineDataFramesPure :: DataFrame cols -> DataFrame cols -> DataFrame cols
combineDataFramesPure (DataFrame dfMap1) (DataFrame dfMap2) =
    DataFrame $ Map.unionWith (V.++) dfMap1 dfMap2

-- Helper function to convert a Stream of single-row DataFrames back to a single DataFrame
streamToDf :: KnownColumns cols => Stream (Of (DataFrame cols)) IO () -> IO (DataFrame cols)
streamToDf stream = do
    list <- S.toList_ stream
    if null list
        then return $ DataFrame Map.empty
        else return $ foldr1 combineDataFramesPure list

isEmpty :: DataFrame cols -> Bool
isEmpty (DataFrame dfMap) = Map.null dfMap

main :: IO ()
main = hspec $ do
    let createTestDataFrame :: IO (DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])
        createTestDataFrame = do
            let rows = [
                    Map.fromList [("Name", TextValue "Alice"), ("Age", IntValue 30), ("Salary", DoubleValue 50000.0)],
                    Map.fromList [("Name", TextValue "Bob"), ("Age", IntValue 25), ("Salary", DoubleValue 60000.0)],
                    Map.fromList [("Name", TextValue "Charlie"), ("Age", IntValue 35), ("Salary", DoubleValue 70000.0)]
                    ]
            return $ fromRows @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] rows

    describe "Type-Safe col function" $ do
        it "correctly infers the type of a column" $ do
            let df = fromRows @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] [
                    Map.fromList [("Name", TextValue "Alice"), ("Age", IntValue 30), ("Salary", DoubleValue 50000.0)]
                    ]
            let ageColExpr = col (Proxy @"Age") :: Expr '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] Int
            let evaluatedAge = evaluateExpr ageColExpr (getDataFrameMap df) 0
            evaluatedAge `shouldBe` Right 30

    describe "Type-Safe filterRows" $ do
        it "filters rows based on a simple predicate" $ do
            df <- createTestDataFrame
            let dfStream = S.yield df
            filteredDf <- streamToDf (Sara.DataFrame.Wrangling.filterRows (col (Proxy @"Age") >.> lit (30 :: Int)) dfStream)
            let (DataFrame filteredMap) = filteredDf
            V.length (filteredMap Map.! "Name") `shouldBe` 1

    describe "Type-Safe applyColumn" $ do
        it "applies a function to a column with the correct type" $ do
            df <- createTestDataFrame
            let dfStream = S.yield df
            appliedDf <- streamToDf (applyColumn (Proxy @"Age") ((+ 2) :: Int -> Int) dfStream)
            let (DataFrame appliedMap) = appliedDf
            case (appliedMap Map.! "Age") V.!? 0 of
                Just (IntValue i) -> i `shouldBe` 32
                _ -> expectationFailure "Expected IntValue for Age"

    describe "Type-Safe sortDataFrame" $ do
        it "sorts a DataFrame by a column" $ do
            df <- createTestDataFrame
            let sortedDf = sortDataFrame [SortCriterion (Proxy @"Age") Ascending] df
            let (DataFrame sortedMap) = sortedDf
            case (sortedMap Map.! "Name") V.!? 0 of
                Just (TextValue t) -> t `shouldBe` "Bob"
                _ -> expectationFailure "Expected TextValue for Name"

    describe "Type-Safe mutate" $ do
        it "adds a new column based on an expression" $ do
            df <- createTestDataFrame
            let expr = col (Proxy @"Age") +.+ lit (5 :: Int)
            let mutatedDfEither = mutate @"AgePlusFive" (Proxy @"AgePlusFive") expr df
            case mutatedDfEither of
                Left err -> expectationFailure $ show err
                Right mutatedDf -> do
                    let (DataFrame mutatedMap) = mutatedDf
                    case (mutatedMap Map.! "AgePlusFive") V.!? 0 of
                        Just (IntValue i) -> i `shouldBe` 35
                        _ -> expectationFailure "Expected IntValue for AgePlusFive"

        

    describe "Type-Safe joinDF" $ do
        let createJoinTestDataFrame1 :: IO (DataFrame '[ '("ID", Int), '("Name", T.Text), '("Age", Int)])
            createJoinTestDataFrame1 = do
                let rows = [
                        Map.fromList [("ID", IntValue 1), ("Name", TextValue "Alice"), ("Age", IntValue 30)],
                        Map.fromList [("ID", IntValue 2), ("Name", TextValue "Bob"), ("Age", IntValue 25)]
                        ]
                return $ fromRows @'[ '("ID", Int), '("Name", T.Text), '("Age", Int)] rows

        let createJoinTestDataFrame2 :: IO (DataFrame '[ '("ID", Int), '("City", T.Text), '("Salary", Double)])
            createJoinTestDataFrame2 = do
                let rows = [
                        Map.fromList [("ID", IntValue 1), ("City", TextValue "New York"), ("Salary", DoubleValue 50000.0)],
                        Map.fromList [("ID", IntValue 3), ("City", TextValue "London"), ("Salary", DoubleValue 70000.0)]
                        ]
                return $ fromRows @'[ '("ID", Int), '("City", T.Text), '("Salary", Double)] rows

        it "performs inner join correctly" $ do
            df1 :: DataFrame '[ '("ID", Int), '("Name", T.Text), '("Age", Int)] <- createJoinTestDataFrame1
            df2 :: DataFrame '[ '("ID", Int), '("City", T.Text), '("Salary", Double)] <- createJoinTestDataFrame2
            let joinedDf = Sara.DataFrame.Join.joinDF @'["ID"] df1 df2
            let (DataFrame joinedMap) = joinedDf
            V.length (joinedMap Map.! "ID") `shouldBe` 1
            case (joinedMap Map.! "Name") V.!? 0 of
                Just (TextValue t) -> t `shouldBe` "Alice"
                _ -> expectationFailure "Expected TextValue for Name"
            case (joinedMap Map.! "City") V.!? 0 of
                Just (TextValue t) -> t `shouldBe` "New York"
                _ -> expectationFailure "Expected TextValue for City"

    describe "Type-Safe dropColumns" $ do
        it "drops specified columns from a DataFrame" $ do
            df <- createTestDataFrame
            let droppedDf = dropColumns @'["Age"] df
            let (DataFrame droppedMap) = droppedDf
            Map.keys droppedMap `shouldNotContain` [T.pack "Age"]
            Map.keys droppedMap `shouldContain` [T.pack "Name", T.pack "Salary"]

    describe "Type-Safe selectColumns" $ do
        it "selects specified columns from a DataFrame" $ do
            df <- createTestDataFrame
            let selectedDf = Sara.DataFrame.Wrangling.selectColumns @'["Name", "Salary"] df
            let (DataFrame selectedMap) = selectedDf
            Map.keys selectedMap `shouldContain` [T.pack "Name", T.pack "Salary"]
            Map.keys selectedMap `shouldNotContain` [T.pack "Age"]

    describe "Type-Safe Aggregation" $ do
        let createAggTestDataFrame :: IO (DataFrame '[ '("Category", T.Text), '("Value", Int)])
            createAggTestDataFrame = do
                let rows = [
                        Map.fromList [("Category", TextValue "A"), ("Value", IntValue 10)],
                        Map.fromList [("Category", TextValue "A"), ("Value", IntValue 20)],
                        Map.fromList [("Category", TextValue "B"), ("Value", IntValue 30)],
                        Map.fromList [("Category", TextValue "B"), ("Value", IntValue 40)]
                        ]
                return $ fromRows @'[ '("Category", T.Text), '("Value", Int)] rows

        it "performs sum aggregation correctly" $ do
            df <- createAggTestDataFrame
            groupedDf <- groupBy @'["Category"] (dfToStream df)
            aggregatedDf <- sumAgg @"Value_sum" @"Value" (Proxy @"Value_sum") (return groupedDf)
            let (DataFrame aggMap) = aggregatedDf
            case (aggMap Map.! "Value_sum") V.!? 0 of
                Just (DoubleValue d) -> d `shouldBe` 30.0
                _ -> expectationFailure "Expected DoubleValue for sumA"
            case (aggMap Map.! "Value_sum") V.!? 1 of
                Just (DoubleValue d) -> d `shouldBe` 70.0
                _ -> expectationFailure "Expected DoubleValue for sumB"

        it "performs mean aggregation correctly" $ do
            df <- createAggTestDataFrame
            groupedDf <- groupBy @'["Category"] (dfToStream df)
            aggregatedDf <- meanAgg @"Value_mean" @"Value" (Proxy @"Value_mean") (return groupedDf)
            let (DataFrame aggMap) = aggregatedDf
            case (aggMap Map.! "Value_mean") V.!? 0 of
                Just (DoubleValue d) -> d `shouldBe` 15.0
                _ -> expectationFailure "Expected DoubleValue for meanA"
            case (aggMap Map.! "Value_mean") V.!? 1 of
                Just (DoubleValue d) -> d `shouldBe` 35.0
                _ -> expectationFailure "Expected DoubleValue for meanB"

        it "performs count aggregation correctly" $ do
            df <- createAggTestDataFrame
            groupedDf <- groupBy @'["Category"] (dfToStream df)
            aggregatedDf <- countAgg @"Category_count" @"Category" (Proxy @"Category_count") (return groupedDf)
            let (DataFrame aggMap) = aggregatedDf
            case (aggMap Map.! "Category_count") V.!? 0 of
                Just (IntValue i) -> i `shouldBe` 2
                _ -> expectationFailure "Expected IntValue for countA"
            case (aggMap Map.! "Category_count") V.!? 1 of
                Just (IntValue i) -> i `shouldBe` 2
                _ -> expectationFailure "Expected IntValue for countB"

    describe "CSV Streaming" $ do
        it "handles empty CSV file" $ do
            withSystemTempFile "empty.csv" $ \filePath handle -> do
                BL.hPutStr handle ""
                hClose handle
                readResult <- readCsvStreaming (Proxy @EmployeesRecord) filePath
                case readResult of
                    Left (ParsingError err) -> err `shouldBe` "parse error (not enough input) at \"\""
                    _ -> expectationFailure "Expected ParsingError for empty CSV file"

        it "handles non-existent CSV file" $ do
            readResult <- readCsvStreaming (Proxy @EmployeesRecord) "non_existent.csv"
            case readResult of
                Left (IOError err) -> T.unpack err `shouldContain` "non_existent.csv"
                _ -> expectationFailure "Expected IOError for non-existent CSV file"

        it "handles malformed CSV file" $ do
            withSystemTempFile "malformed.csv" $ \filePath handle -> do
                BL.hPutStr handle "col1,col2\nval1\n"
                hClose handle
                readResult <- readCsvStreaming (Proxy @EmployeesRecord) filePath
                case readResult of
                    Left (ParsingError err) -> T.unpack err `shouldContain` "conversion error: no field named \"EmployeeID\""
                    _ -> expectationFailure "Expected ParsingError for malformed CSV file"

    describe "JSON Streaming" $ do
        let testDataFrame = fromRows @'[ '("name", T.Text), '("age", Int)] [
                Map.fromList [("name", TextValue "Alice"), ("age", IntValue 30)],
                Map.fromList [("name", TextValue "Bob"), ("age", IntValue 25)]
                ]

        it "writes and reads JSON in a streaming fashion" $ do
            withSystemTempFile "test.json" $ \filePath handle -> do
                BL.hPutStr handle ""
                hClose handle
                writeJSONStreaming filePath (dfToStream testDataFrame)
                readDfStreamEither <- liftIO $ readJSONStreaming (Proxy @'[ '("name", T.Text), '("age", Int)]) filePath
                case readDfStreamEither of
                    Left err -> expectationFailure $ show err
                    Right readDfStream -> do
                        readDf <- streamToDf readDfStream
                        readDf `shouldBe` testDataFrame

        it "handles non-existent JSON file" $ do
            readResult <- readJSONStreaming (Proxy @'[ '("name", T.Text), '("age", Int)]) "non_existent.json"
            case readResult of
                Left (IOError err) -> T.unpack err `shouldContain` "non_existent.json"
                _ -> expectationFailure "Expected IOError for non-existent JSON file"

        it "handles malformed JSON file" $ do
            withSystemTempFile "malformed.json" $ \filePath handle -> do
                BL.hPutStr handle "[{"
                hClose handle
                readResult <- readJSONStreaming (Proxy @'[ '("name", T.Text), '("age", Int)]) filePath
                case readResult of
                    Left (ParsingError err) -> T.unpack err `shouldContain` "Unexpected end-of-input, expecting record key literal or }"
                    _ -> expectationFailure "Expected ParsingError for malformed JSON file"

    describe "DFValue JSON Serialization/Deserialization" $ do
        it "serializes and deserializes IntValue" $ do
            let dfVal = IntValue 123
            A.decode (A.encode dfVal) `shouldBe` Just dfVal
        it "converts Int to DFValue and back" $ do
            let val = 123 :: Int
            fromDFValue (toDFValue val) `shouldBe` Right val
        it "converts Double to DFValue and back" $ do
            let val = 123.45 :: Double
            fromDFValue (toDFValue val) `shouldBe` Right val
        it "converts Text to DFValue and back" $ do
            let val = "hello" :: T.Text
            fromDFValue (toDFValue val) `shouldBe` Right val
        it "converts Bool to DFValue and back" $ do
            let val = True :: Bool
            fromDFValue (toDFValue val) `shouldBe` Right val
        it "converts Day to DFValue and back" $ do
            let val = fromGregorian 2023 1 1 :: Day
            fromDFValue (toDFValue val) `shouldBe` Right val
        it "converts UTCTime to DFValue and back" $ do
            let val = UTCTime (fromGregorian 2023 1 1) (secondsToDiffTime 3600) :: UTCTime
            fromDFValue (toDFValue val) `shouldBe` Right val
        it "handles NA for Maybe type" $ do
            let val = Nothing :: Maybe Int
            fromDFValue (toDFValue val) `shouldBe` Right val
        it "handles Just value for Maybe type" $ do
            let val = Just 123 :: Maybe Int
            fromDFValue (toDFValue val) `shouldBe` Right val
        
        

    

    describe "isNA function" $ do
        it "returns True for NA" $ do
            isNA NA `shouldBe` True
        it "returns False for IntValue" $ do
            isNA (IntValue 1) `shouldBe` False
        it "returns False for TextValue" $ do
            isNA (TextValue "a") `shouldBe` False

    describe "getDFValueType function" $ do
        it "returns correct TypeRep for IntValue" $ do
            getDFValueType (IntValue 1) `shouldBe` Just (typeRep (Proxy @Int))
        it "returns correct TypeRep for DoubleValue" $ do
            getDFValueType (DoubleValue 1.0) `shouldBe` Just (typeRep (Proxy @Double))
        it "returns correct TypeRep for TextValue" $ do
            getDFValueType (TextValue "a") `shouldBe` Just (typeRep (Proxy @T.Text))
        it "returns correct TypeRep for BoolValue" $ do
            getDFValueType (BoolValue True) `shouldBe` Just (typeRep (Proxy @Bool))
        it "returns correct TypeRep for DateValue" $ do
            getDFValueType (DateValue (fromGregorian 2023 1 1)) `shouldBe` Just (typeRep (Proxy @Day))
        it "returns correct TypeRep for TimestampValue" $ do
            getDFValueType (TimestampValue (UTCTime (fromGregorian 2023 1 1) (secondsToDiffTime 0))) `shouldBe` Just (typeRep (Proxy @UTCTime))
        it "returns Nothing for NA" $ do
            getDFValueType NA `shouldBe` Nothing

    describe "DataFrame fromRows and toRows" $ do
        it "fromRows . toRows is identity for non-empty DataFrame" $ do
            let df = fromRows @'[ '("A", Int), '("B", T.Text)] [Map.fromList [("A", IntValue 1), ("B", TextValue "a")]]
            (fromRows . toRows) df `shouldBe` df
        it "fromRows . toRows is identity for empty DataFrame" $ do
            let df = fromRows @'[ '("A", Int)] []
            (fromRows . toRows) df `shouldBe` df
        it "toRows returns empty list for empty DataFrame" $ do
            let df = DataFrame Map.empty :: DataFrame '[ '("A", Int)]
            toRows df `shouldBe` []
        it "fromRows handles rows with missing columns by filling with NA" $ do
            let rows = [
                    Map.fromList [("colA", IntValue 1), ("colB", TextValue "a")],
                    Map.fromList [("colA", IntValue 2)] -- Missing colB
                    ]
            let df = fromRows @'[ '("colA", Int), '("colB", T.Text)] rows
            let expectedRows = [
                    Map.fromList [("colA", IntValue 1), ("colB", TextValue "a")],
                    Map.fromList [("colA", IntValue 2), ("colB", NA)]
                    ]
            toRows df `shouldBe` expectedRows
        it "fromRows ignores extra columns not in schema" $ do
            let rows = [
                    Map.fromList [("colA", IntValue 1), ("colB", TextValue "a"), ("colC", IntValue 3)] -- Extra colC
                    ]
            let df = fromRows @'[ '("colA", Int), '("colB", T.Text)] rows
            let expectedRows = [
                    Map.fromList [("colA", IntValue 1), ("colB", TextValue "a")]
                    ]
            toRows df `shouldBe` expectedRows

    describe "QuickCheck Properties" $ do
        prop "fromRows . toRows is identity" (prop_fromRows_toRows_identity @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])

    describe "Expression Evaluation" $ do
        let testDataFrameMap = Map.fromList [
                ("colA", V.fromList [IntValue 10, IntValue 20, IntValue 30]),
                ("colB", V.fromList [IntValue 2, IntValue 4, IntValue 6]),
                ("colC", V.fromList [TextValue "hello", TextValue "world", TextValue "haskell"]),
                ("colD", V.fromList [DoubleValue 10.0, DoubleValue 20.0, DoubleValue 30.0]),
                ("colE", V.fromList [DoubleValue 0.0, DoubleValue 1.0, DoubleValue 2.0]),
                ("colF", V.fromList [BoolValue True, BoolValue False, BoolValue True])
                ]

        it "evaluates Lit correctly" $ do
            evaluateExpr (lit (5 :: Int)) testDataFrameMap 0 `shouldBe` Right 5
            evaluateExpr (lit ("test" :: T.Text)) testDataFrameMap 0 `shouldBe` Right "test"

        it "evaluates Col correctly" $ do
            evaluateExpr (col (Proxy @"colA") :: Expr '[ '("colA", Int)] Int) testDataFrameMap 0 `shouldBe` Right 10
            evaluateExpr (col (Proxy @"colC") :: Expr '[ '("colC", T.Text)] T.Text) testDataFrameMap 1 `shouldBe` Right "world"

        it "handles ColumnNotFound error" $ do
            -- To test ColumnNotFound at runtime, we create an Expr that refers to a column
            -- that is part of the *type-level* schema, but then provide a dfMap that *lacks* that column.
            let missingColMap = Map.fromList [
                    ("colA", V.fromList [IntValue 10])
                    ]
            evaluateExpr (col (Proxy @"colB") :: Expr '[ '("colA", Int), '("colB", Int)] Int) missingColMap 0 `shouldBe` Left (ColumnNotFound "colB")

        it "evaluates Add correctly" $ do
            evaluateExpr (lit (5 :: Int) +.+ lit (3 :: Int)) testDataFrameMap 0 `shouldBe` Right 8
            evaluateExpr (col (Proxy @"colA") +.+ lit (5 :: Int) :: Expr '[ '("colA", Int)] Int) testDataFrameMap 0 `shouldBe` Right 15

        it "evaluates Subtract correctly" $ do
            evaluateExpr (lit (5 :: Int) -.- lit (3 :: Int)) testDataFrameMap 0 `shouldBe` Right 2
            evaluateExpr (col (Proxy @"colA") -.- lit (5 :: Int) :: Expr '[ '("colA", Int)] Int) testDataFrameMap 0 `shouldBe` Right 5

        it "evaluates Multiply correctly" $ do
            evaluateExpr (lit (5 :: Int) *.* lit (3 :: Int)) testDataFrameMap 0 `shouldBe` Right 15
            evaluateExpr (col (Proxy @"colA") *.* lit (2 :: Int) :: Expr '[ '("colA", Int)] Int) testDataFrameMap 0 `shouldBe` Right 20

        it "evaluates Divide correctly" $ do
            evaluateExpr (lit (10 :: Double) /.! lit (2 :: Double)) testDataFrameMap 0 `shouldBe` Right 5.0
            evaluateExpr (col (Proxy @"colD") /.! lit (2 :: Double) :: Expr '[ '("colD", Double)] Double) testDataFrameMap 0 `shouldBe` Right 5.0

        it "evaluates GreaterThan correctly" $ do
            evaluateExpr (lit (10 :: Int) >. lit (5 :: Int)) testDataFrameMap 0 `shouldBe` Right True
            evaluateExpr (lit (5 :: Int) >. lit (10 :: Int)) testDataFrameMap 0 `shouldBe` Right False
            evaluateExpr (col (Proxy @"colA") >. lit (5 :: Int) :: Expr '[ '("colA", Int)] Bool) testDataFrameMap 0 `shouldBe` Right True

        it "evaluates LessThan correctly" $ do
            evaluateExpr (lit (5 :: Int) <. lit (10 :: Int)) testDataFrameMap 0 `shouldBe` Right True
            evaluateExpr (lit (10 :: Int) <. lit (5 :: Int)) testDataFrameMap 0 `shouldBe` Right False
            evaluateExpr (col (Proxy @"colA") <. lit (5 :: Int) :: Expr '[ '("colA", Int)] Bool) testDataFrameMap 0 `shouldBe` Right False

        it "evaluates EqualTo correctly" $ do
            evaluateExpr (lit (5 :: Int) ===. lit (5 :: Int)) testDataFrameMap 0 `shouldBe` Right True
            evaluateExpr (lit (5 :: Int) ===. lit (10 :: Int)) testDataFrameMap 0 `shouldBe` Right False
            evaluateExpr (col (Proxy @"colA") ===. lit (10 :: Int) :: Expr '[ '("colA", Int)] Bool) testDataFrameMap 0 `shouldBe` Right True

        it "evaluates GreaterThanOrEqualTo correctly" $ do
            evaluateExpr (lit (10 :: Int) >=. lit (10 :: Int)) testDataFrameMap 0 `shouldBe` Right True
            evaluateExpr (lit (5 :: Int) >=. lit (10 :: Int)) testDataFrameMap 0 `shouldBe` Right False

        it "evaluates And correctly" $ do
            evaluateExpr (lit True &&. lit True) testDataFrameMap 0 `shouldBe` Right True
            evaluateExpr (lit True &&. lit False) testDataFrameMap 0 `shouldBe` Right False
            evaluateExpr (col (Proxy @"colF") &&. lit True :: Expr '[ '("colF", Bool)] Bool) testDataFrameMap 1 `shouldBe` Right False

        it "evaluates Or correctly" $ do
            evaluateExpr (lit True ||. lit False) testDataFrameMap 0 `shouldBe` Right True
            evaluateExpr (lit False ||. lit False) testDataFrameMap 0 `shouldBe` Right False
            evaluateExpr (col (Proxy @"colF") ||. lit False :: Expr '[ '("colF", Bool)] Bool) testDataFrameMap 1 `shouldBe` Right False

        it "evaluates ApplyFn correctly" $ do
            evaluateExpr (ApplyFn (+1) (lit (5 :: Int))) testDataFrameMap 0 `shouldBe` Right 6
            evaluateExpr (ApplyFn T.reverse (lit ("olleh" :: T.Text))) testDataFrameMap 0 `shouldBe` Right "hello"

    describe "DataFrame Concatenation" $ do
        let df1 = fromRows @'[ '("colA", Int), '("colB", T.Text)] [
                Map.fromList [("colA", IntValue 1), ("colB", TextValue "a")]
                ]
        let df2 = fromRows @'[ '("colA", Int), '("colB", T.Text)] [
                Map.fromList [("colA", IntValue 2), ("colB", TextValue "b")]
                ]
        let emptyDf = DataFrame Map.empty

        it "concatenates two non-empty DataFrames by rows" $ do
            let resultDf = concatDF (Proxy @'[ '("colA", Int), '("colB", T.Text)]) ConcatRows [df1, df2]
            toRows resultDf `shouldBe` [
                Map.fromList [("colA", IntValue 1), ("colB", TextValue "a")],
                Map.fromList [("colA", IntValue 2), ("colB", TextValue "b")]
                ]

        it "concatenates an empty DataFrame with a non-empty one by rows" $ do
            let resultDf = concatDF (Proxy @'[ '("colA", Int), '("colB", T.Text)]) ConcatRows [emptyDf, df1]
            toRows resultDf `shouldBe` toRows df1

        it "concatenates a non-empty DataFrame with an empty one by rows" $ do
            let resultDf = concatDF (Proxy @'[ '("colA", Int), '("colB", T.Text)]) ConcatRows [df1, emptyDf]
            toRows resultDf `shouldBe` toRows df1

        it "concatenates multiple empty DataFrames by rows" $ do
            let resultDf = concatDF (Proxy @'[ '("colA", Int), '("colB", T.Text)]) ConcatRows [emptyDf, emptyDf]
            isEmpty resultDf `shouldBe` True

        it "concatenates two non-empty DataFrames by columns (identical schemas)" $ do
            let dfA = fromRows @'[ '("colA", Int), '("colB", T.Text)] [Map.fromList [("colA", IntValue 1), ("colB", TextValue "x")]]
            let dfB = fromRows @'[ '("colA", Int), '("colB", T.Text)] [Map.fromList [("colA", IntValue 2), ("colB", TextValue "y")]]
            let resultDf = concatDF (Proxy @'[ '("colA", Int), '("colB", T.Text)]) ConcatColumns [dfA, dfB]
            -- Note: For ConcatColumns with identical schemas, the behavior is effectively a union of columns.
            -- If column names overlap, the later DataFrame's values for that column will overwrite earlier ones.
            -- This test verifies that behavior for identical schemas.
            toRows resultDf `shouldBe` [Map.fromList [("colA", IntValue 2), ("colB", TextValue "y")]]

        it "handles overlapping columns in ConcatColumns (last one wins, identical schemas)" $ do
            let dfX = fromRows @'[ '("colA", Int), '("colB", T.Text)] [Map.fromList [("colA", IntValue 1), ("colB", TextValue "x")]]
            let dfY = fromRows @'[ '("colA", Int), '("colB", T.Text)] [Map.fromList [("colA", IntValue 2), ("colB", TextValue "y")]]
            let resultDf = concatDF (Proxy @'[ '("colA", Int), '("colB", T.Text)]) ConcatColumns [dfX, dfY]
            toRows resultDf `shouldBe` [Map.fromList [("colA", IntValue 2), ("colB", TextValue "y")]]

    describe "SQL Integration" $ do
        let withTempDb :: (FilePath -> IO a) -> IO a
            withTempDb action = withSystemTempFile "test.db" $ \filePath handle -> do
                hClose handle -- Close the handle immediately, as sqlite opens it itself
                action filePath

        it "reads data from a simple SQL table" $ do
            withTempDb $ \dbPath -> do
                conn <- open dbPath
                execute_ conn "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
                execute_ conn "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
                execute_ conn "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"
                close conn

                let expectedSchema = Proxy @'[ '("id", Int), '("name", T.Text), '("age", Int)]
                resultEither <- readSQL expectedSchema dbPath "SELECT id, name, age FROM users"
                case resultEither of
                    Left err -> expectationFailure $ show err
                    Right df -> do
                        let expectedRows = [
                                Map.fromList [("id", IntValue 1), ("name", TextValue "Alice"), ("age", IntValue 30)],
                                Map.fromList [("id", IntValue 2), ("name", TextValue "Bob"), ("age", IntValue 25)]
                                ]
                        toRows df `shouldBe` expectedRows

        it "reads data from an empty SQL table" $ do
            withTempDb $ \dbPath -> do
                conn <- open dbPath
                execute_ conn "CREATE TABLE empty_table (col1 INTEGER, col2 TEXT)"
                close conn

                let expectedSchema = Proxy @'[ '("col1", Int), '("col2", T.Text)]
                resultEither <- readSQL expectedSchema dbPath "SELECT col1, col2 FROM empty_table"
                case resultEither of
                    Left err -> expectationFailure $ show err
                    Right df -> isEmpty df `shouldBe` True

        it "reads data with different types including Bool and NA" $ do
            withTempDb $ \dbPath -> do
                conn <- open dbPath
                execute_ conn "CREATE TABLE mixed_types (id INTEGER, name TEXT, active BOOLEAN, value REAL, description TEXT)"
                execute_ conn "INSERT INTO mixed_types (id, name, active, value, description) VALUES (1, 'Test1', 'true', 1.23, NULL)"
                execute_ conn "INSERT INTO mixed_types (id, name, active, value, description) VALUES (2, 'Test2', 'false', 4.56, 'some text')"
                close conn

                let expectedSchema = Proxy @'[ '("id", Int), '("name", T.Text), '("active", Bool), '("value", Double), '("description", T.Text)]
                resultEither <- readSQL expectedSchema dbPath "SELECT id, name, active, value, description FROM mixed_types"
                case resultEither of
                    Left err -> expectationFailure $ show err
                    Right df -> do
                        let expectedRows = [
                                Map.fromList [("id", IntValue 1), ("name", TextValue "Test1"), ("active", BoolValue True), ("value", DoubleValue 1.23), ("description", NA)],
                                Map.fromList [("id", IntValue 2), ("name", TextValue "Test2"), ("active", BoolValue False), ("value", DoubleValue 4.56), ("description", TextValue "some text")]]
                        toRows df `shouldBe` expectedRows

        it "handles non-existent database file (no such table)" $ do
            let expectedSchema = Proxy @'[ '("id", Int)]
            resultEither <- readSQL expectedSchema "non_existent_temp.db" "SELECT id FROM users"
            case resultEither of
                Left (GenericError err) -> T.unpack err `shouldContain` "no such table"
                _ -> expectationFailure "Expected GenericError for non-existent table in new DB"

        it "handles column count mismatch" $ do
            withTempDb $ \dbPath -> do
                conn <- open dbPath
                execute_ conn "CREATE TABLE test_table (col1 INTEGER, col2 TEXT)"
                execute_ conn "INSERT INTO test_table (col1, col2) VALUES (1, 'a')"
                close conn

                let expectedSchema = Proxy @'[ '("col1", Int)] -- Expecting only one column
                resultEither <- readSQL expectedSchema dbPath "SELECT col1, col2 FROM test_table"
                case resultEither of
                    Left (GenericError err) -> T.unpack err `shouldContain` "column count mismatch"
                    _ -> expectationFailure "Expected GenericError for column count mismatch"

        it "handles type mismatch" $ do
            withTempDb $ \dbPath -> do
                conn <- open dbPath
                execute_ conn "CREATE TABLE test_table (col1 INTEGER)"
                execute_ conn "INSERT INTO test_table (col1) VALUES ('not_an_int')"
                close conn

                let expectedSchema = Proxy @'[ '("col1", Int)]
                resultEither <- readSQL expectedSchema dbPath "SELECT col1 FROM test_table"
                case resultEither of
                    Left (TypeMismatch expected actual) -> do
                        expected `shouldBe` "Int"
                        T.unpack actual `shouldContain` "not_an_int"
                    _ -> expectationFailure "Expected TypeMismatch for type mismatch"

        it "handles unsupported SQL type (BLOB)" $ do
            withTempDb $ \dbPath -> do
                conn <- open dbPath
                execute_ conn "CREATE TABLE test_table (col1 BLOB)"
                execute_ conn "INSERT INTO test_table (col1) VALUES (X'010203')"
                close conn

                let expectedSchema = Proxy @'[ '("col1", Int)] -- Type doesn't matter, it's about the BLOB
                resultEither <- readSQL expectedSchema dbPath "SELECT col1 FROM test_table"
                case resultEither of
                    Left (GenericError err) -> err `shouldBe` "Unsupported SQL type: BLOB"
                    _ -> expectationFailure "Expected GenericError for unsupported BLOB type"

    describe "Missing Data Handling" $ do
        let dfWithNAs = fromRows @'[ '("colA", Int), '("colB", T.Text), '("colC", Double)] [
                Map.fromList [("colA", IntValue 1), ("colB", TextValue "a"), ("colC", DoubleValue 1.1)],
                Map.fromList [("colA", NA), ("colB", TextValue "b"), ("colC", NA)],
                Map.fromList [("colA", IntValue 3), ("colB", NA), ("colC", DoubleValue 3.3)],
                Map.fromList [("colA", NA), ("colB", NA), ("colC", NA)]
                ]
        let dfNoNAs = fromRows @'[ '("colA", Int)] [
                Map.fromList [("colA", IntValue 1)],
                Map.fromList [("colA", IntValue 2)]
                ]
        let emptyDf = DataFrame Map.empty

        it "fills NA values in a specific column" $ do
            let filledDf = fillna dfWithNAs (Proxy @Int) (Just "colA") (99 :: Int)
            let expectedRows = [
                    Map.fromList [("colA", IntValue 1), ("colB", TextValue "a"), ("colC", DoubleValue 1.1)],
                    Map.fromList [("colA", IntValue 99), ("colB", TextValue "b"), ("colC", NA)],
                    Map.fromList [("colA", IntValue 3), ("colB", NA), ("colC", DoubleValue 3.3)],
                    Map.fromList [("colA", IntValue 99), ("colB", NA), ("colC", NA)]
                    ]
            toRows filledDf `shouldBe` expectedRows

        it "fills NA values in all columns" $ do
            let filledDf = fillna dfWithNAs (Proxy @Int) Nothing (0 :: Int)
            let expectedRows = [
                    Map.fromList [("colA", IntValue 1), ("colB", TextValue "a"), ("colC", DoubleValue 1.1)],
                    Map.fromList [("colA", IntValue 0), ("colB", TextValue "b"), ("colC", IntValue 0)],
                    Map.fromList [("colA", IntValue 3), ("colB", IntValue 0), ("colC", DoubleValue 3.3)],
                    Map.fromList [("colA", IntValue 0), ("colB", IntValue 0), ("colC", IntValue 0)]
                    ]
            toRows filledDf `shouldBe` expectedRows

        it "does nothing if no NAs are present" $ do
            let filledDf = fillna dfNoNAs (Proxy @Int) Nothing (99 :: Int)
            filledDf `shouldBe` dfNoNAs

        it "handles empty DataFrame for fillna" $ do
            let filledDf = fillna (emptyDf :: DataFrame '[]) (Proxy @Int) Nothing (99 :: Int)
            filledDf `shouldBe` (emptyDf :: DataFrame '[])

        it "performs ffill (forward fill)" $ do
            let df = fromRows @'[ '("colA", Int)] [
                    Map.fromList [("colA", IntValue 1)],
                    Map.fromList [("colA", NA)],
                    Map.fromList [("colA", IntValue 3)],
                    Map.fromList [("colA", NA)]
                    ]
            let filledDf = ffill df
            let expectedRows = [
                    Map.fromList [("colA", IntValue 1)],
                    Map.fromList [("colA", IntValue 1)],
                    Map.fromList [("colA", IntValue 3)],
                    Map.fromList [("colA", IntValue 3)]
                    ]
            toRows filledDf `shouldBe` expectedRows

        it "performs bfill (backward fill)" $ do
            let df = fromRows @'[ '("colA", Int)] [
                    Map.fromList [("colA", NA)],
                    Map.fromList [("colA", IntValue 2)],
                    Map.fromList [("colA", NA)],
                    Map.fromList [("colA", IntValue 4)]
                    ]
            let filledDf = bfill df
            let expectedRows = [
                    Map.fromList [("colA", IntValue 2)],
                    Map.fromList [("colA", IntValue 2)],
                    Map.fromList [("colA", IntValue 4)],
                    Map.fromList [("colA", IntValue 4)]
                    ]
            toRows filledDf `shouldBe` expectedRows

        it "drops rows with any NA values" $ do
            let droppedDf = dropna dfWithNAs DropRows Nothing
            let expectedRows = [
                    Map.fromList [("colA", IntValue 1), ("colB", TextValue "a"), ("colC", DoubleValue 1.1)]
                    ]
            toRows droppedDf `shouldBe` expectedRows

        it "drops rows based on threshold" $ do
            let droppedDf = dropna dfWithNAs DropRows (Just 2) -- Requires at least 2 non-NA values
            let expectedRows = [
                    Map.fromList [("colA", IntValue 1), ("colB", TextValue "a"), ("colC", DoubleValue 1.1)],
                    Map.fromList [("colA", IntValue 3), ("colB", NA), ("colC", DoubleValue 3.3)]
                    ]
            toRows droppedDf `shouldBe` expectedRows

        it "drops columns with any NA values" $ do
            let droppedDf = dropna dfWithNAs DropColumns Nothing
            toRows droppedDf `shouldBe` []

        it "drops columns based on threshold" $ do
            let droppedDf = dropna dfWithNAs DropColumns (Just 3) -- Requires at least 3 non-NA values
            toRows droppedDf `shouldBe` []

        it "returns isna DataFrame" $ do
            let isnaDf = isna dfWithNAs
            let expectedRows = [
                    Map.fromList [("colA", BoolValue False), ("colB", BoolValue False), ("colC", BoolValue False)],
                    Map.fromList [("colA", BoolValue True), ("colB", BoolValue False), ("colC", BoolValue True)],
                    Map.fromList [("colA", BoolValue False), ("colB", BoolValue True), ("colC", BoolValue False)],
                    Map.fromList [("colA", BoolValue True), ("colB", BoolValue True), ("colC", BoolValue True)]
                    ]
            toRows isnaDf `shouldBe` expectedRows

        it "returns notna DataFrame" $ do
            let notnaDf = notna dfWithNAs
            let expectedRows = [
                    Map.fromList [("colA", BoolValue True), ("colB", BoolValue True), ("colC", BoolValue True)],
                    Map.fromList [("colA", BoolValue False), ("colB", BoolValue True), ("colC", BoolValue False)],
                    Map.fromList [("colA", BoolValue True), ("colB", BoolValue False), ("colC", BoolValue True)],
                    Map.fromList [("colA", BoolValue False), ("colB", BoolValue False), ("colC", BoolValue False)]
                    ]
            toRows notnaDf `shouldBe` expectedRows

    describe "Granular QuickCheck Properties" $ do
        prop "filterRows with a tautology predicate is identity" (prop_filterRows_tautology @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])
        prop "filterRows with a contradiction predicate is empty" (prop_filterRows_contradiction @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])
        prop "selectColumns preserves values" $
            \(df :: DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)]) ->
                prop_selectColumns_preserves_values (Proxy @'["Name", "Age"]) df
        prop "dropColumns preserves values in remaining columns" $
            \(df :: DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)]) ->
                prop_dropColumns_preserves_values (Proxy @'["Age"]) df
        
        prop "sortDataFrame preserves rows and sorts correctly" $
            forAll (arbitrary :: Gen (DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])) $ \df ->
                forAll (arbitrarySortCriteriaFixed @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)]) $ \criteria ->
                    prop_sortDataFrame_core df criteria
        prop "joinDF produces correct IDs" (prop_joinDF_correct_ids @'[ '("ID", Int), '("Name", T.Text)] @'[ '("ID", Int), '("City", T.Text)])
        prop "applyColumn applies a function correctly" (prop_applyColumn_plus_one)
        prop "mutate adds a column correctly" (prop_mutate_correctness)
        prop "sumAgg calculates sum correctly" (prop_sumAgg_correctness)
        prop "meanAgg calculates mean correctly" (prop_meanAgg_correctness)
        prop "countAgg calculates count correctly" (prop_countAgg_correctness)

prop_applyColumn_plus_one :: DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] -> Property
prop_applyColumn_plus_one df = ioProperty $ do
    let f = (+1) :: Int -> Int
    appliedDf <- streamToDf (applyColumn (Proxy @"Age") f (dfToStream df))
    let originalAges = map (fromRight' . fromDFValue @Int . (Map.! "Age")) (toRows df)
    let appliedAges = map (fromRight' . fromDFValue @Int . (Map.! "Age")) (toRows appliedDf)
    let expectedAges = map f originalAges

    -- also check that other columns are untouched
    let originalNames = map (fromRight' . fromDFValue @T.Text . (Map.! "Name")) (toRows df)
    let appliedNames = map (fromRight' . fromDFValue @T.Text . (Map.! "Name")) (toRows appliedDf)

    return $ (appliedAges === expectedAges) .&&. (originalNames === appliedNames)

prop_mutate_correctness :: DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] -> Property
prop_mutate_correctness df = ioProperty $ do
    let expr = col (Proxy @"Age") +.+ lit (5 :: Int)
    let mutatedDfEither = mutate (Proxy @"AgePlusFive") expr df
    case mutatedDfEither of
        Left _ -> return $ property False
        Right mutatedDf -> do
            let originalAges = map (fromRight' . fromDFValue @Int . (Map.! "Age")) (toRows df)
            let newColValues = map (fromRight' . fromDFValue @Int . (Map.! "AgePlusFive")) (toRows mutatedDf)
            let expectedValues = map (+5) originalAges
            return $ newColValues === expectedValues .&&. Map.member "AgePlusFive" (getDataFrameMap mutatedDf)


prop_sumAgg_correctness :: DataFrame '[ '("Category", T.Text), '("Value", Int)] -> Property
prop_sumAgg_correctness df = not (isEmpty df) ==> ioProperty $ do
    groupedStream <- groupBy @'["Category"] (dfToStream df)
    aggregatedDf <- sumAgg @"Value_sum" @"Value" (Proxy @"Value_sum") (return groupedStream)

    let originalRows = toRows df
    let expectedSumMap = Map.fromListWith (+) $ map (\row ->
            ( fromRight' (fromDFValue @T.Text (row Map.! "Category"))
            , fromIntegral @Int @Double $ fromRight' (fromDFValue @Int (row Map.! "Value"))
            )) originalRows

    let aggregatedRows = toRows aggregatedDf
    let actualSumMap = Map.fromList $ map (\row ->
            ( fromRight' (fromDFValue @T.Text (row Map.! "Category"))
            , fromRight' (fromDFValue @Double (row Map.! "Value_sum"))
            )) aggregatedRows

    return $ actualSumMap === expectedSumMap



prop_meanAgg_correctness :: DataFrame '[ '("Category", T.Text), '("Value", Int)] -> Property
prop_meanAgg_correctness df = not (isEmpty df) ==> ioProperty $ do
    groupedStream <- groupBy @'["Category"] (dfToStream df)
    aggregatedDf <- meanAgg @"Value_mean" @"Value" (Proxy @"Value_mean") (return groupedStream)

    let originalRows = toRows df
    let valueMap = Map.fromListWith (++) $ map (\row ->
            ( fromRight' (fromDFValue @T.Text (row Map.! "Category"))
            , [fromIntegral @Int @Double $ fromRight' (fromDFValue @Int (row Map.! "Value"))]
            )) originalRows
    let expectedMeanMap = Map.map (\vals -> sum vals / fromIntegral (length vals)) valueMap

    let aggregatedRows = toRows aggregatedDf
    let actualMeanMap = Map.fromList $ map (\row ->
            ( fromRight' (fromDFValue @T.Text (row Map.! "Category"))
            , fromRight' (fromDFValue @Double (row Map.! "Value_mean"))
            )) aggregatedRows

    return $ actualMeanMap === expectedMeanMap



prop_countAgg_correctness :: DataFrame '[ '("Category", T.Text), '("Value", Int)] -> Property
prop_countAgg_correctness df = not (isEmpty df) ==> ioProperty $ do
    groupedStream <- groupBy @'["Category"] (dfToStream df)
    aggregatedDf <- countAgg @"Category_count" @"Category" (Proxy @"Category_count") (return groupedStream)

    let originalRows = toRows df
    let expectedCountMap = Map.fromListWith (+) $ map (\row ->
            ( fromRight' (fromDFValue @T.Text (row Map.! "Category"))
            , 1 :: Int
            )) originalRows

    let aggregatedRows = toRows aggregatedDf
    let actualCountMap = Map.fromList $ map (\row ->
            ( fromRight' (fromDFValue @T.Text (row Map.! "Category"))
            , fromRight' (fromDFValue @Int (row Map.! "Category_count"))
            )) aggregatedRows

    return $ actualCountMap === expectedCountMap



prop_arbitrary_dataframe_type_awareness :: forall cols. (KnownColumns cols, Arbitrary (DataFrame cols)) => DataFrame cols -> Property
prop_arbitrary_dataframe_type_awareness df = property $ do
    let (DataFrame dfMap) = df
    all (\(colName, colType) ->
        case Map.lookup (T.pack colName) dfMap of
            Just vec -> all (isCorrectType colType) (V.toList vec)
            Nothing -> True
        ) (map (\(n, t) -> (T.unpack n, t)) (columnSchema (Proxy @cols)))
    where
        isCorrectType :: TypeRep -> DFValue -> Bool
        isCorrectType tr (IntValue _) = tr == typeRep (Proxy @Int)
        isCorrectType tr (TextValue _) = tr == typeRep (Proxy @T.Text)
        isCorrectType tr (DoubleValue _) = tr == typeRep (Proxy @Double)
        isCorrectType tr (BoolValue _) = tr == typeRep (Proxy @Bool)
        isCorrectType _ NA = True -- NA can be any type
        isCorrectType _ (DateValue _) = True -- Placeholder
        isCorrectType _ (TimestampValue _) = True -- Placeholder

prop_filterRows_tautology :: (KnownColumns cols, Arbitrary (DataFrame cols)) => DataFrame cols -> Property
prop_filterRows_tautology df = ioProperty $ do
    filteredDf <- streamToDf (filterRows (FilterPredicate (ExprPredicate (lit True))) (dfToStream df))
    return $ filteredDf === df

prop_filterRows_contradiction :: (KnownColumns cols, Arbitrary (DataFrame cols)) => DataFrame cols -> Property
prop_filterRows_contradiction df = ioProperty $ do
    filteredDf <- streamToDf (filterRows (FilterPredicate (ExprPredicate (lit False))) (dfToStream df))
    return $ toRows filteredDf === []

prop_selectColumns_preserves_values :: forall (selectedCols :: [Symbol]) allCols.
    ( KnownColumns allCols, KnownColumns (SelectCols selectedCols allCols)
    , Arbitrary (DataFrame allCols)
    , HasColumns selectedCols allCols
    , AllKnownSymbol selectedCols
    , All CanBeDFValue (GetColumnTypes allCols)
    , All CanBeDFValue (GetColumnTypes (SelectCols selectedCols allCols))
    ) => Proxy selectedCols -> DataFrame allCols -> Property
prop_selectColumns_preserves_values _ df =
    let selectedDf = selectColumns @selectedCols df
        originalRows = toRows df
        selectedRows = toRows selectedDf
    in  property $ L.length originalRows == L.length selectedRows &&
        all (\(originalRow, selectedRow) ->
            all (\colName ->
                Map.lookup colName originalRow ==
                Map.lookup colName selectedRow
            ) (symbolsToTexts (Proxy @selectedCols))
        ) (zip originalRows selectedRows)

prop_dropColumns_preserves_values :: forall droppedCols allCols.
    ( KnownColumns allCols, KnownColumns (DropColumns droppedCols allCols)
    , Arbitrary (DataFrame allCols)
    , AllKnownSymbol droppedCols
    , All CanBeDFValue (GetColumnTypes allCols)
    , All CanBeDFValue (GetColumnTypes (DropColumns droppedCols allCols))
    , AllKnownSymbol (MapFst (DropColumns droppedCols allCols))
    ) => Proxy droppedCols -> DataFrame allCols -> Property
prop_dropColumns_preserves_values _ df =
    let droppedDf = dropColumns @droppedCols df
        originalRows = toRows df
        droppedRows = toRows droppedDf
        remainingCols = Sara.DataFrame.Wrangling.symbolsToTexts (Proxy @(MapFst (DropColumns droppedCols allCols)))
    in  property $ L.length originalRows == L.length droppedRows &&
        all (\(originalRow, droppedRow) ->
            all (\colName ->
                Map.lookup colName originalRow ==
                Map.lookup colName droppedRow
            ) remainingCols
        ) (zip originalRows droppedRows)

arbitrarySortCriteriaFixed :: forall cols. (KnownColumns cols, HasColumn "Name" cols, TypeOf "Name" cols ~ T.Text) => Gen [SortCriterion cols]
arbitrarySortCriteriaFixed = do
    order <- elements [Ascending, Descending]
    return [SortCriterion (Proxy @"Name") order] -- Using a fixed column for simplicity

prop_sortDataFrame_core :: forall cols.
    ( KnownColumns cols, Arbitrary (DataFrame cols)
    , HasColumn "Name" cols, TypeOf "Name" cols ~ T.Text
    , HasColumn "Age" cols, TypeOf "Age" cols ~ Int
    , HasColumn "Salary" cols, TypeOf "Salary" cols ~ Double
    , All CanBeDFValue (GetColumnTypes cols)
    ) => DataFrame cols -> [SortCriterion cols] -> Property
prop_sortDataFrame_core df criteria =
    let sortedDf = sortDataFrame criteria df
        originalRows = toRows df
        sortedRows = toRows sortedDf
    in  property $ L.length originalRows == L.length sortedRows &&
        isSorted criteria sortedRows originalRows
    where
        isSorted :: [SortCriterion cols] -> [Row] -> [Row] -> Bool
        isSorted [] _ _ = True
        isSorted (crit:_) sorted original =
            let compareRows' r1 r2 = case crit of
                    SortCriterion (_ :: Proxy col) Ascending ->
                        let val1 = Map.lookup (T.pack $ symbolVal (Proxy @col)) r1
                            val2 = Map.lookup (T.pack $ symbolVal (Proxy @col)) r2
                        in compare val1 val2
                    SortCriterion (_ :: Proxy col) Descending ->
                        let val1 = Map.lookup (T.pack $ symbolVal (Proxy @col)) r1
                            val2 = Map.lookup (T.pack $ symbolVal (Proxy @col)) r2
                        in compare val2 val1
            in  all (\(r1, r2) -> compareRows' r1 r2 /= GT) (zip sorted (tail sorted)) &&
                sortBy compareRows' original == sorted

prop_joinDF_correct_ids :: forall cols1 cols2. (KnownColumns cols1, KnownColumns cols2, Arbitrary (DataFrame cols1), Arbitrary (DataFrame cols2), HasColumn "ID" cols1, HasColumn "ID" cols2, TypeOf "ID" cols1 ~ Int, TypeOf "ID" cols2 ~ Int, KnownColumns (JoinCols cols1 cols2), All CanBeDFValue (GetColumnTypes cols1), All CanBeDFValue (GetColumnTypes cols2), All CanBeDFValue (GetColumnTypes (JoinCols cols1 cols2)), CreateOutputRow (JoinCols cols1 cols2)) => DataFrame cols1 -> DataFrame cols2 -> Property
prop_joinDF_correct_ids df1 df2 = property $ do
    let joinedDf = joinDF @'["ID"] df1 df2
    let joinedIDs = Set.fromList $ map (fromRight' . fromDFValue @Int . (Map.! "ID")) (toRows joinedDf)
    let df1IDs = Set.fromList $ map (fromRight' . fromDFValue @Int . (Map.! "ID")) (toRows df1)
    let df2IDs = Set.fromList $ map (fromRight' . fromDFValue @Int . (Map.! "ID")) (toRows df2)
    let expectedIDs = Set.intersection df1IDs df2IDs
    joinedIDs === expectedIDs
