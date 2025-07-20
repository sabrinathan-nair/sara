{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}

module Main where

import Test.Hspec
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.Transform
import Sara.DataFrame.Types
import Sara.DataFrame.Wrangling
import Sara.DataFrame.Aggregate
import Sara.DataFrame.Expression
import Sara.DataFrame.Predicate
import Sara.DataFrame.Join
import Data.Proxy (Proxy(..))

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

    describe "Type-Safe filterRows" $ do
        it "filters rows based on a simple predicate" $ do
            df <- createTestDataFrame
            let filteredDf = filterRows (col (Proxy @"Age") >.> lit (30 :: Int)) df
            let (DataFrame filteredMap) = filteredDf
            V.length (filteredMap Map.! "Name") `shouldBe` 1

    describe "Type-Safe applyColumn" $ do
        it "applies a function to a column with the correct type" $ do
            df <- createTestDataFrame
            let appliedDf = applyColumn (Proxy @"Age") ((+ 2) :: Int -> Int) df
            let (DataFrame appliedMap) = appliedDf
            let age = case (appliedMap Map.! "Age") V.!? 0 of
                          Just (IntValue i) -> i
                          _ -> error "Expected IntValue for Age" -- Should not happen in this test
            age `shouldBe` 32

    describe "Type-Safe sortDataFrame" $ do
        it "sorts a DataFrame by a column" $ do
            df <- createTestDataFrame
            let sortedDf = sortDataFrame [SortCriterion (Proxy @"Age") Ascending] df
            let (DataFrame sortedMap) = sortedDf
            let name = case (sortedMap Map.! "Name") V.!? 0 of
                          Just (TextValue t) -> t
                          _ -> error "Expected TextValue for Name"
            name `shouldBe` "Bob"

    describe "Type-Safe mutate" $ do
        it "adds a new column based on an expression" $ do
            df <- createTestDataFrame
            let expr = (col (Proxy @"Age") :: Expr '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] Int) +.+ lit (5 :: Int)
            let mutatedDf = mutate @"AgePlusFive" (Proxy @"AgePlusFive") expr df
            let (DataFrame mutatedMap) = mutatedDf
            let val = case (mutatedMap Map.! "AgePlusFive") V.!? 0 of
                          Just (IntValue i) -> i
                          _ -> error "Expected IntValue for AgePlusFive"
            val `shouldBe` 35

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
            df1 <- createJoinTestDataFrame1
            df2 <- createJoinTestDataFrame2
            let joinedDf = Sara.DataFrame.Join.joinDF @'["ID"] df1 df2
            let (DataFrame joinedMap) = joinedDf
            V.length (joinedMap Map.! "ID") `shouldBe` 1
            let name = case (joinedMap Map.! "Name") V.!? 0 of
                          Just (TextValue t) -> t
                          _ -> error "Expected TextValue for Name"
            name `shouldBe` "Alice"
            let city = case (joinedMap Map.! "City") V.!? 0 of
                          Just (TextValue t) -> t
                          _ -> error "Expected TextValue for City"
            city `shouldBe` "New York"

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
            let groupedDf = groupBy @'["Category"] df
            let aggregatedDf = sumAgg @"Value" groupedDf
            let (DataFrame aggMap) = aggregatedDf
            let sumA = case (aggMap Map.! "Value_sum") V.!? 0 of
                          Just (DoubleValue d) -> d
                          _ -> error "Expected DoubleValue for sumA"
            let sumB = case (aggMap Map.! "Value_sum") V.!? 1 of
                          Just (DoubleValue d) -> d
                          _ -> error "Expected DoubleValue for sumB"
            sumA `shouldBe` 30.0
            sumB `shouldBe` 70.0

        it "performs mean aggregation correctly" $ do
            df <- createAggTestDataFrame
            let groupedDf = groupBy @'["Category"] df
            let aggregatedDf = meanAgg @"Value" groupedDf
            let (DataFrame aggMap) = aggregatedDf
            let meanA = case (aggMap Map.! "Value_mean") V.!? 0 of
                          Just (DoubleValue d) -> d
                          _ -> error "Expected DoubleValue for meanA"
            let meanB = case (aggMap Map.! "Value_mean") V.!? 1 of
                          Just (DoubleValue d) -> d
                          _ -> error "Expected DoubleValue for meanB"
            meanA `shouldBe` 15.0
            meanB `shouldBe` 35.0

        it "performs count aggregation correctly" $ do
            df <- createAggTestDataFrame
            let groupedDf = groupBy @'["Category"] df
            let aggregatedDf = countAgg @"Value" groupedDf
            let (DataFrame aggMap) = aggregatedDf
            let countA = case (aggMap Map.! "Value_count") V.!? 0 of
                          Just (IntValue i) -> i
                          _ -> error "Expected IntValue for countA"
            let countB = case (aggMap Map.! "Value_count") V.!? 1 of
                          Just (IntValue i) -> i
                          _ -> error "Expected IntValue for countB"
            countA `shouldBe` 2
            countB `shouldBe` 2

    
