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

import Data.Text ()

import Data.Time.Calendar ()
import GHC.Generics ()
import Data.Csv ()
import Sara.DataFrame.Internal ()
import Data.Aeson



$(inferCsvSchema "Employees" True "employees.csv")

instance FromJSON EmployeesRecord

$(inferCsvSchema "Departments" True "departments.csv")

instance FromJSON DepartmentsRecord