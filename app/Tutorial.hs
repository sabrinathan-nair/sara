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

module Main where

import Data.Time (UTCTime, fromGregorian, UTCTime(..))
import Sara.DataFrame.Static (inferCsvSchema)




-- Helper function to create UTCTime values
createUTCTime :: Integer -> Int -> Int -> UTCTime
createUTCTime y m d = UTCTime (fromGregorian y m d) 0

$(inferCsvSchema "Employees" "employees.csv")
$(inferCsvSchema "Departments" "departments.csv")

main :: IO ()
main = return ()