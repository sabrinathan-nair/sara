{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Main where

import Sara.DataFrame.Static
import Data.Csv
import qualified Data.Vector as V
import GHC.Generics (Generic)

-- | Generate the Employee type from the employees.csv file.
tableTypes "Employee" "employees.csv"

main :: IO ()
main = do
    putStrLn "Reading employees.csv with static types..."
    employees <- readCsv "employees.csv"
    case employees of
        Left err -> putStrLn err
        Right (records :: V.Vector Employee) -> V.forM_ records print