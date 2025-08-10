{-# LANGUAGE OverloadedStrings #-}

module Sara.Core.Types (
    EmployeeID,
    DepartmentName,
    Email,
    Salary,
    mkEmployeeID,
    mkDepartmentName,
    mkEmail,
    mkSalary,
    unEmployeeID,
    unSalary
) where

import Data.Text (Text)
import qualified Data.Text as T
import Sara.Error (SaraError(..))

-- | Represents an employee ID.
newtype EmployeeID = EmployeeID Int deriving (Show, Eq, Ord)

unEmployeeID :: EmployeeID -> Int
unEmployeeID (EmployeeID i) = i

-- | Smart constructor for `EmployeeID`.
-- Validates that the ID is a positive integer.
mkEmployeeID :: Int -> Either SaraError EmployeeID
mkEmployeeID eid
    | eid > 0 = Right (EmployeeID eid)
    | otherwise = Left (InvalidArgument "Employee ID must be positive")

-- | Represents a department name.
newtype DepartmentName = DepartmentName Text deriving (Show, Eq, Ord)

-- | Smart constructor for `DepartmentName`.
-- Validates that the name is not empty and is not longer than 50 characters.
mkDepartmentName :: Text -> Either SaraError DepartmentName
mkDepartmentName dn
    | T.null dn = Left (InvalidArgument "Department name cannot be empty")
    | T.length dn > 50 = Left (InvalidArgument "Department name cannot be longer than 50 characters")
    | otherwise = Right (DepartmentName dn)

-- | Represents an email address.
newtype Email = Email Text deriving (Show, Eq, Ord)

-- | Smart constructor for `Email`.
-- Validates that the email address contains an '@' symbol.
mkEmail :: Text -> Either SaraError Email
mkEmail email
    | '@' `T.elem` email = Right (Email email)
    | otherwise = Left (InvalidArgument "Invalid email address")

-- | Represents a salary.
newtype Salary = Salary Double deriving (Show, Eq, Ord)

unSalary :: Salary -> Double
unSalary (Salary s) = s

-- | Smart constructor for `Salary`.
-- Validates that the salary is a positive number.
mkSalary :: Double -> Either SaraError Salary
mkSalary s
    | s > 0 = Right (Salary s)
    | otherwise = Left (InvalidArgument "Salary must be positive")
