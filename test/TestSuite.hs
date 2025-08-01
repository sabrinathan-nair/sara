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
import Data.Proxy (Proxy(..))
import Streaming (Stream, Of)
import qualified Streaming.Prelude as S
import Sara.DataFrame.IO (readJSONStreaming, writeJSONStreaming, readCsvStreaming)
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
streamToDf :: KnownColumns cols => Stream (Of (Either SaraError (DataFrame cols))) IO () -> IO (Either SaraError (DataFrame cols))
streamToDf stream = do
    list <- S.toList_ stream
    let (errList, dfsList) = partitionEithers list
    if null errList
        then if null dfsList
            then return $ Right $ DataFrame Map.empty
            else return $ Right $ foldr1 combineDataFramesPure dfsList
        else return $ Left $ head errList

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
            filteredDfEither <- streamToDf $ Sara.DataFrame.Wrangling.filterRows (col (Proxy @"Age") >.> lit (30 :: Int)) dfStream
            case filteredDfEither of
                Left err -> expectationFailure $ show err
                Right filteredDf -> do
                    let (DataFrame filteredMap) = filteredDf
                    V.length (filteredMap Map.! "Name") `shouldBe` 1

    describe "Type-Safe applyColumn" $ do
        it "applies a function to a column with the correct type" $ do
            df <- createTestDataFrame
            let dfStream = S.yield df
            appliedDfEither <- streamToDf $ applyColumn (Proxy @"Age") ((+ 2) :: Int -> Int) dfStream
            case appliedDfEither of
                Left err -> expectationFailure $ show err
                Right appliedDf -> do
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
            let joinedDfStream = Sara.DataFrame.Join.joinDF @'["ID"] (dfToStream df1) (dfToStream df2)
            joinedDfEither <- streamToDf joinedDfStream
            case joinedDfEither of
                Left err -> expectationFailure $ show err
                Right joinedDf -> do
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
            aggregatedDf <- sumAgg @"Value" (return groupedDf)
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
            aggregatedDf <- meanAgg @"Value" (return groupedDf)
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
            aggregatedDf <- countAgg @"Category" (return groupedDf)
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
                        readDfEither <- streamToDf (S.map Right readDfStream)
                        case readDfEither of
                            Left err -> expectationFailure $ show err
                            Right readDf -> readDf `shouldBe` testDataFrame

    describe "QuickCheck Properties" $ do
        prop "fromRows . toRows is identity" (prop_fromRows_toRows_identity @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])

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
    appliedDfEither <- streamToDf $ applyColumn (Proxy @"Age") f (dfToStream df)
    case appliedDfEither of
        Left _ -> return $ property False
        Right appliedDf -> do
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
    aggregatedDf <- sumAgg @"Value" (return groupedStream)

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
    aggregatedDf <- meanAgg @"Value" (return groupedStream)

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
    aggregatedDf <- countAgg @"Category" (return groupedStream)

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
    filteredDfEither <- streamToDf $ filterRows (FilterPredicate (ExprPredicate (lit True))) (dfToStream df)
    case filteredDfEither of
        Left _ -> return $ property False
        Right filteredDf -> return $ filteredDf === df

prop_filterRows_contradiction :: (KnownColumns cols, Arbitrary (DataFrame cols)) => DataFrame cols -> Property
prop_filterRows_contradiction df = ioProperty $ do
    filteredDfEither <- streamToDf $ filterRows (FilterPredicate (ExprPredicate (lit False))) (dfToStream df)
    case filteredDfEither of
        Left _ -> return $ property False
        Right filteredDf -> return $ toRows filteredDf === []

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
prop_joinDF_correct_ids df1 df2 = ioProperty $ do
    joinedDfEither <- streamToDf $ joinDF @'["ID"] (dfToStream df1) (dfToStream df2)
    case joinedDfEither of
        Left _ -> return $ property False
        Right joinedDf -> do
            let joinedIDs = Set.fromList $ map (fromRight' . fromDFValue @Int . (Map.! "ID")) (toRows joinedDf)
            let df1IDs = Set.fromList $ map (fromRight' . fromDFValue @Int . (Map.! "ID")) (toRows df1)
            let df2IDs = Set.fromList $ map (fromRight' . fromDFValue @Int . (Map.! "ID")) (toRows df2)
            let expectedIDs = Set.intersection df1IDs df2IDs
            return $ joinedIDs === expectedIDs



























