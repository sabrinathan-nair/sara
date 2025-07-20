{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Criterion.Main
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import Sara.DataFrame.Types
import Sara.DataFrame.Wrangling
import Sara.DataFrame.Predicate
import Sara.DataFrame.Expression (col, lit)
import Data.Proxy (Proxy(..))

createTestDataFrame :: IO (DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])
createTestDataFrame = do
    let rows = [
            Map.fromList [("Name", TextValue "Alice"), ("Age", IntValue 30), ("Salary", DoubleValue 50000.0)],
            Map.fromList [("Name", TextValue "Bob"), ("Age", IntValue 25), ("Salary", DoubleValue 60000.0)],
            Map.fromList [("Name", TextValue "Charlie"), ("Age", IntValue 35), ("Salary", DoubleValue 70000.0)]
            ]
    return $ fromRows @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] rows

main :: IO ()
main = defaultMain [
  bgroup "filterRows" [
    bench "filter by age > 30" $ nfIO $ do
      df <- createTestDataFrame
      return $ filterRows (col (Proxy @"Age") >.> lit (30 :: Int)) df
    ]
  ]
