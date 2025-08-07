{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Sara.Validation.Employee (
    ValidatedEmployee(..),
    validateEmployee
) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Calendar (Day)
import Control.Monad.Trans.Except (ExceptT(..), runExceptT)
import Control.Monad.IO.Class (liftIO)
import Control.Monad (when)

import Sara.Error (SaraError(..))
import Sara.DataFrame.Types (EmployeeID, DepartmentName, Email, Salary, mkEmployeeID, mkDepartmentName, mkEmail, mkSalary)
import Sara.Schema.Definitions (EmployeesRecord)

-- | Represents a validated employee record.
data ValidatedEmployee = ValidatedEmployee
    { veEmployeeID :: EmployeeID
    , veName :: Text
    , veDepartmentName :: DepartmentName
    , veSalary :: Salary
    , veStartDate :: Day
    , veEmail :: Email
    } deriving (Show, Eq)

-- | Validates an employee record.
validateEmployee :: (Int, Text, Text, Double, Day, Text) -> Either [SaraError] ValidatedEmployee
validateEmployee (employeeID, name, departmentName, salary, startDate, email) =
    case (mkEmployeeID employeeID, mkDepartmentName departmentName, mkSalary salary, mkEmail email) of
        (Right eid, Right dn, Right sal, Right em) ->
            Right ValidatedEmployee
                { veEmployeeID = eid
                , veName = name
                , veDepartmentName = dn
                , veSalary = sal
                , veStartDate = startDate
                , veEmail = em
                }
        (eidErr, dnErr, salErr, emErr) ->
            Left $ concat
                [ either (pure . id) (const []) eidErr
                , either (pure . id) (const []) dnErr
                , either (pure . id) (const []) salErr
                , either (pure . id) (const []) emErr
                ]
