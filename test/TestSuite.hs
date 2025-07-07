{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Test.Hspec
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.IO (readCSV)
import Sara.DataFrame.Transform
import Sara.DataFrame.Types
import Sara.DataFrame.Wrangling
import Sara.DataFrame.Expression
import Sara.DataFrame.Predicate
import Data.Proxy (Proxy(..))

main :: IO ()
main = hspec $ do
    let createTestDataFrame :: IO (DataFrame '["Name", "Age", "Salary"])
        createTestDataFrame = do
            let rows = [
                    Map.fromList [("Name", TextValue "Alice"), ("Age", IntValue 30), ("Salary", DoubleValue 50000.0)],
                    Map.fromList [("Name", TextValue "Bob"), ("Age", IntValue 25), ("Salary", DoubleValue 60000.0)],
                    Map.fromList [("Name", TextValue "Charlie"), ("Age", IntValue 35), ("Salary", DoubleValue 70000.0)]
                    ]
            return $ fromRows @'["Name", "Age", "Salary"] rows

    describe "Type-Safe filterRows" $ do
        it "filters rows based on a simple predicate" $ do
            df <- createTestDataFrame
            let predicate = (col (Proxy @"Age") :: Expr '["Name", "Age", "Salary"] Int) >. lit 30
            let filteredDf = filterRows (Proxy @"Age" >.> IntValue 30) df
            let (DataFrame filteredMap) = filteredDf
            V.length (filteredMap Map.! "Name") `shouldBe` 1

    describe "Type-Safe applyColumn" $ do
        it "applies a function to a column with the correct type" $ do
            df <- createTestDataFrame
            let appliedDf = applyColumn @"Age" @Int (* 2) df
            let (DataFrame appliedMap) = appliedDf
            let (Just (IntValue age)) = (appliedMap Map.! "Age") V.!? 0
            age `shouldBe` 60

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
            let expr = (col (Proxy @"Age") :: Expr '["Name", "Age", "Salary"] Int) +.+ lit 5
            let mutatedDf = mutate (Proxy @"AgePlusFive") expr df
            let (DataFrame mutatedMap) = mutatedDf
            let (Just (IntValue val)) = (mutatedMap Map.! "AgePlusFive") V.!? 0
            val `shouldBe` 35
