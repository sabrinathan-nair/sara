{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}

module Sara.Schema.Definitions (
    EmployeesRecord,
    DepartmentsRecord,
    Employees,
    Departments
) where

import Sara.DataFrame.Static (inferCsvSchema)

import Data.Text (Text)

import Data.Time.Calendar (Day)
import GHC.Generics (Generic)
import Data.Csv (FromNamedRecord)
import Sara.DataFrame.Internal (HasSchema, Schema, HasTypeName, getTypeName)
import Data.Aeson



$(inferCsvSchema "Employees" True "employees.csv")

instance FromJSON EmployeesRecord

$(inferCsvSchema "Departments" True "departments.csv")

instance FromJSON DepartmentsRecord