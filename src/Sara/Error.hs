{-# LANGUAGE OverloadedStrings #-}

-- | This module defines the custom error types for the Sara library.
module Sara.Error (
    SaraError(..),
    ValidationError(..)
) where

import qualified Data.Text as T

-- | A custom error type for the Sara library.
-- This allows for more specific and helpful error messages.
data SaraError =
    -- | A parsing error, with a descriptive message.
    ParsingError T.Text |
    -- | An arithmetic error, with a descriptive message.
    ArithmeticError T.Text |
    -- | A column not found error, with the column name.
    ColumnNotFound T.Text |
    -- | A type mismatch error, with the expected and actual types.
    TypeMismatch T.Text T.Text |
    -- | An IO error, with a descriptive message.
    IOError T.Text |
    -- | An error for an operation on an empty DataFrame.
    EmptyDataFrameError T.Text |
    -- | An error for invalid arguments provided to a function.
    InvalidArgument T.Text |
    -- | An error for an operation on an empty column.
    EmptyColumn T.Text T.Text |
    -- | A generic error, with a descriptive message.
    GenericError T.Text
    deriving (Eq)

data ValidationError =
    InvalidEmail T.Text |
    MissingField T.Text |
    NegativeSalary Double
    deriving (Eq)

instance Show ValidationError where
    show (InvalidEmail email) = "Invalid email: " ++ T.unpack email
    show (MissingField field) = "Missing field: " ++ T.unpack field
    show (NegativeSalary salary) = "Negative salary: " ++ show salary

instance Show SaraError where
    show (ParsingError msg) = "Parsing Error: " ++ T.unpack msg
    show (ArithmeticError msg) = "Arithmetic Error: " ++ T.unpack msg
    show (ColumnNotFound col) = "Column Not Found: " ++ T.unpack col
    show (TypeMismatch expected actual) = "Type Mismatch: Expected " ++ T.unpack expected ++ ", but got " ++ T.unpack actual
    show (IOError msg) = "IO Error: " ++ T.unpack msg
    show (EmptyDataFrameError msg) = "Empty DataFrame Error: " ++ T.unpack msg
    show (InvalidArgument msg) = "Invalid Argument: " ++ T.unpack msg
    show (EmptyColumn colName msg) = "Empty Column Error for '" ++ T.unpack colName ++ "': " ++ T.unpack msg
    show (GenericError msg) = "Error: " ++ T.unpack msg
