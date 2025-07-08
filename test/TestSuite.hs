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
import Sara.DataFrame.IO (readCSV)
import Sara.DataFrame.Transform
import Sara.DataFrame.Types
import Sara.DataFrame.Wrangling
import Sara.DataFrame.Aggregate
import Sara.DataFrame.Expression
import Sara.DataFrame.Predicate
import Data.Proxy (Proxy(..))
import Data.Kind (Type)

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
            let predicate = (col (Proxy @"Age") :: Expr '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] Int) >. lit (30 :: Int)
            let filteredDf = filterRows (col (Proxy @"Age") >.> lit (30 :: Int)) df
            let (DataFrame filteredMap) = filteredDf
            V.length (filteredMap Map.! "Name") `shouldBe` 1

    describe "Type-Safe applyColumn" $ do
        it "applies a function to a column with the correct type" $ do
            df <- createTestDataFrame
            let appliedDf = applyColumn (Proxy @"Age") ((+ 2) :: Int -> Int) df
            let (DataFrame appliedMap) = appliedDf
            let (Just (IntValue age)) = (appliedMap Map.! "Age") V.!? 0
            age `shouldBe` 32

    describe "Type-Safe sortDataFrame" $ do
        it "sorts a DataFrame by a column" $ do
            df <- createTestDataFrame
            let sortedDf = sortDataFrame [SortCriterion (Proxy @"Age") Ascending] df
            let (DataFrame sortedMap) = sortedDf
            let (Just (TextValue name)) = (sortedMap Map.! "Name") V.!? 0
            name `shouldBe` "Bob"

    describe "Type-Safe mutate" $ do
        it "adds a new column based on an expression" $ do
            df <- createTestDataFrame
            let expr = (col (Proxy @"Age") :: Expr '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] Int) +.+ lit (5 :: Int)
            let mutatedDf = mutate @'("AgePlusFive", Int) (Proxy @"AgePlusFive") expr df
            let (DataFrame mutatedMap) = mutatedDf
            let (Just (IntValue val)) = (mutatedMap Map.! "AgePlusFive") V.!? 0
            val `shouldBe` 35

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
            let groupedDf = groupBy @'[ '("Category", T.Text)] df
            let aggregatedDf = sumAgg @"Value" @'[ '("Category", T.Text)] groupedDf
            let (DataFrame aggMap) = aggregatedDf
            let (Just (DoubleValue sumA)) = (aggMap Map.! "Value_sum") V.!? 0
            let (Just (DoubleValue sumB)) = (aggMap Map.! "Value_sum") V.!? 1
            sumA `shouldBe` 30.0
            sumB `shouldBe` 70.0

        it "performs mean aggregation correctly" $ do
            df <- createAggTestDataFrame
            let groupedDf = groupBy @'[ '("Category", T.Text)] df
            let aggregatedDf = meanAgg @"Value" @'[ '("Category", T.Text)] groupedDf
            let (DataFrame aggMap) = aggregatedDf
            let (Just (DoubleValue meanA)) = (aggMap Map.! "Value_mean") V.!? 0
            let (Just (DoubleValue meanB)) = (aggMap Map.! "Value_mean") V.!? 1
            meanA `shouldBe` 15.0
            meanB `shouldBe` 35.0

        it "performs count aggregation correctly" $ do
            df <- createAggTestDataFrame
            let groupedDf = groupBy @'[ '("Category", T.Text)] df
            let aggregatedDf = countAgg @"Value" @'[ '("Category", T.Text)] groupedDf
            let (DataFrame aggMap) = aggregatedDf
            let (Just (IntValue countA)) = (aggMap Map.! "Value_count") V.!? 0
            let (Just (IntValue countB)) = (aggMap Map.! "Value_count") V.!? 1
            countA `shouldBe` 2
            countB `shouldBe` 2
