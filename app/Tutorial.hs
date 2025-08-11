{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Main where


import Sara.DataFrame.IO (readCsvStreaming)
import qualified Streaming.Prelude as S
import Sara.DataFrame.Wrangling (filterByBoolColumn)
import Sara.DataFrame.Transform (mutate)
import Sara.DataFrame.Expression (col, lit, (>.))
import Data.Proxy
import Sara.DataFrame.Static (inferCsvSchema)
import Data.Text (Text)
import qualified Data.ByteString.Char8 as BC
import qualified Data.Csv as C
import Data.Time.Calendar (Day)
import GHC.Generics (Generic)
import Data.Csv (FromNamedRecord)
import Sara.DataFrame.Internal (HasSchema, Schema, HasTypeName, getTypeName)

$(inferCsvSchema "Employees" False "employees.csv")


tutorial :: IO ()
tutorial = do
    readResult <- readCsvStreaming (Proxy @EmployeesRecord) "employees.csv"

    case readResult of
        Left err -> putStrLn $ "Error reading CSV: " ++ show err
        Right dfStream -> do
            S.mapM_ (\dfEither -> case dfEither of
                Left err -> putStrLn $ "Error processing DataFrame: " ++ show err
                Right df -> do
                    let mutatedDfEither = mutate (Proxy :: Proxy "IsSalaryHigh") (col (Proxy @"Salary") >. lit 70000) df
                    case mutatedDfEither of
                        Left err -> putStrLn $ "Error mutating: " ++ show err
                        Right mutatedDf -> do
                            let filteredDf = filterByBoolColumn (Proxy :: Proxy "IsSalaryHigh") (S.yield mutatedDf)
                            S.mapM_ print filteredDf
                ) dfStream

main :: IO ()
main = tutorial