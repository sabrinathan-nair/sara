{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import Data.Time (UTCTime, fromGregorian, UTCTime(..))
import Sara.DataFrame.Static (inferCsvSchema)




-- Helper function to create UTCTime values
createUTCTime :: Integer -> Int -> Int -> UTCTime
createUTCTime y m d = UTCTime (fromGregorian y m d) 0

$(inferCsvSchema "EmployeesSchema" "employees.csv")
$(inferCsvSchema "DepartmentsSchema" "departments.csv")
$(inferCsvSchema "SQLEmployeesSchema" "employees.csv")

main :: IO ()
main = return ()