{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Main where


import qualified Data.ByteString.Char8 as BC
import qualified Data.Csv as C


import Sara.DataFrame.Static (inferCsvSchema)
import Sara.DataFrame.IO (readCsvStreaming)
import qualified Streaming.Prelude as S
import Sara.DataFrame.Wrangling (filterByBoolColumn)
import Sara.DataFrame.Transform (mutate)
import Sara.DataFrame.Expression (col, lit, (>.))
import Data.Proxy
import Sara.DataFrame.Internal (HasSchema, Schema, HasTypeName, getTypeName)
import Data.Time.Calendar (Day)
import Data.Text (Text)
import Data.Csv (FromNamedRecord)
import GHC.Generics (Generic)



tutorial :: IO ()
tutorial = do
    readResult <- readCsvStreaming (Proxy @EmployeesRecord) "employees.csv"

    case readResult of
        Left err -> putStrLn $ "Error reading CSV: " ++ show err
        Right dfStream -> do
            S.mapM_ (\df -> do
                let mutatedDfEither = mutate (Proxy :: Proxy "IsSalaryHigh") (col (Proxy @"EmployeesSalary") >. lit 70000) df
                case mutatedDfEither of
                    Left err -> putStrLn $ "Error mutating: " ++ show err
                    Right mutatedDf -> do
                        let filteredDfEither = filterByBoolColumn (Proxy :: Proxy "IsSalaryHigh") (S.yield mutatedDf)
                        S.mapM_ (\e -> case e of
                            Left err -> putStrLn $ "Error filtering: " ++ show err
                            Right filteredDf -> print filteredDf
                            ) filteredDfEither
                ) dfStream

main :: IO ()
main = tutorial