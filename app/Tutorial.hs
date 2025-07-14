{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeFamilies #-}

module Main where

import Data.Time (UTCTime, fromGregorian, UTCTime(..))
import Sara.DataFrame.Static (inferCsvSchema, readCsv)
import Sara.DataFrame.Types
import Sara.DataFrame.Wrangling
import Sara.DataFrame.Transform
import Sara.DataFrame.Aggregate
import Sara.DataFrame.Expression (col, lit, (*.*), (>.), (&&.))
import Data.Proxy
import Sara.DataFrame.Internal (ToDataFrameRecord(..), HasSchema(..))
import qualified Data.Vector as V
import GHC.Generics
import Data.Csv (FromNamedRecord)

-- Helper function to create UTCTime values
createUTCTime :: Integer -> Int -> Int -> UTCTime
createUTCTime y m d = UTCTime (fromGregorian y m d) 0

$(inferCsvSchema "Employees" "employees.csv")
$(inferCsvSchema "Departments" "departments.csv")

tutorial :: IO ()
tutorial = do
    Right records <- readCsv "employees.csv" :: IO (Either String (V.Vector EmployeesRecord))
    let df = toDataFrame records
    Right records' <- readCsv "departments.csv" :: IO (Either String (V.Vector DepartmentsRecord))
    let df' = toDataFrame records'

    let groupedDf = groupBy @'[] df
    let groupedDf = groupBy @'[] df
    let totalSalary = sumAgg @"EmployeesSalary" groupedDf

    -- This should fail to compile because of CanBeDFValue instance
    let df2 = applyColumn (Proxy :: Proxy "EmployeesSalary") (* 2) df

    -- This should fail to compile because of argument handling
    let df3 = filterByBoolColumn (Proxy :: Proxy "EmployeesSalary") df
    
    print (columnNames (Proxy :: Proxy Employees))
    

main :: IO ()
main = return ()