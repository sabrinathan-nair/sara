{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

-- | This is the main module for the Sara application.
-- It demonstrates how to use the `tableTypes` Template Haskell function
-- to generate a record type from a CSV file and then read the CSV file
-- into a `Vector` of those records.
module Main where

import Sara.DataFrame.Static
import Data.Csv
import qualified Data.Vector as V
import GHC.Generics (Generic)

-- | Generate the `Employee` type from the `employees.csv` file.
-- The `tableTypes` function is a Template Haskell function that generates
-- a record type with fields corresponding to the columns in the CSV file.
tableTypes "Employee" "employees.csv"

-- | The main entry point for the application.
main :: IO ()
main = do
    putStrLn "Reading employees.csv with static types..."
    employees <- readCsv "employees.csv"
    case employees of
        Left err -> putStrLn err
        Right (records :: V.Vector Employee) -> V.forM_ records print