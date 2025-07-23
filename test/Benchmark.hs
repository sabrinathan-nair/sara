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
import Streaming (Stream, Of)
import qualified Streaming.Prelude as S
import Control.Monad.IO.Class (liftIO)
import qualified Data.Vector as V

createTestDataFrame :: IO (DataFrame '[ '("Name", T.Text), '("Age", Int), '("Salary", Double)])
createTestDataFrame = do
    let rows = [
            Map.fromList [("Name", TextValue "Alice"), ("Age", IntValue 30), ("Salary", DoubleValue 50000.0)],
            Map.fromList [("Name", TextValue "Bob"), ("Age", IntValue 25), ("Salary", DoubleValue 60000.0)],
            Map.fromList [("Name", TextValue "Charlie"), ("Age", IntValue 35), ("Salary", DoubleValue 70000.0)]
            ]
    return $ fromRows @'[ '("Name", T.Text), '("Age", Int), '("Salary", Double)] rows

-- Helper function to convert a DataFrame to a Stream of single-row DataFrames
dfToStream :: DataFrame cols -> Stream (Of (DataFrame cols)) IO ()
dfToStream df = S.each (map (\row -> DataFrame (Map.map V.singleton row)) (toRows df))

-- Helper function to combine two DataFrames
combineDataFrames :: DataFrame cols -> DataFrame cols -> IO (DataFrame cols)
combineDataFrames (DataFrame dfMap1) (DataFrame dfMap2) =
    return $ DataFrame $ Map.unionWith (V.++) dfMap1 dfMap2

-- Helper function to convert a Stream of single-row DataFrames back to a single DataFrame
streamToDf :: KnownColumns cols => Stream (Of (DataFrame cols)) IO () -> IO (DataFrame cols)
streamToDf stream =
    S.foldM_ combineDataFrames (return (DataFrame Map.empty)) return stream

main :: IO ()
main = defaultMain [
  bgroup "filterRows" [
    bench "filter by age > 30" $ nfIO $ do
      df <- createTestDataFrame
      filteredStream <- streamToDf $ filterRows (col (Proxy @"Age") >.> lit (30 :: Int)) (dfToStream df)
      return filteredStream
    ]
  ]
