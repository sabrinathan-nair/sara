{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}

module Main where

import Criterion.Main
import Sara.DataFrame.IO
import Sara.DataFrame.Types
import Data.Proxy (Proxy(..))

-- Define a sample record type for benchmarking
-- This should match the structure of your test CSV/JSON files
-- For example, if your data has columns "name" (Text) and "age" (Int)
data Person = Person { name :: Text, age :: Int }

main :: IO ()
main = defaultMain [
    bgroup "Streaming Benchmarks" [
        bgroup "CSV" [
            bench "readCsv" $ nfIO (readCsv (Proxy :: Proxy Person) "people.csv"),
            bench "readCsvStreaming" $ nfIO (readCsvStreaming (Proxy :: Proxy Person) "people.csv")
        ],
        bgroup "JSON" [
            bench "readJSON" $ nfIO (readJSON (Proxy :: Proxy Person) "people.json"),
            bench "readJSONStreaming" $ nfIO (readJSONStreaming (Proxy :: Proxy Person) "people.json")
        ]
    ]
    ]
