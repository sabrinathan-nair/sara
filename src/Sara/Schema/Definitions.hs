{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}

module Sara.Schema.Definitions where

import Sara.DataFrame.Static (inferCsvSchema)
import Data.Text (Text)
import qualified Data.ByteString.Char8 as BC
import qualified Data.Csv as C
import Data.Time.Calendar (Day)
import Data.Time.Clock (UTCTime)
import GHC.Generics (Generic)
import Data.Csv (FromNamedRecord)
import Sara.DataFrame.Internal (HasSchema, Schema, HasTypeName, getTypeName)

$(inferCsvSchema "Employees" "employees.csv")
$(inferCsvSchema "Departments" "departments.csv")
