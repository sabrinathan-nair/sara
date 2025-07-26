{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE UndecidableInstances #-}

-- | This module defines a type-safe GADT for building and evaluating expressions
-- on `DataFrame` rows. It provides a way to construct complex, type-checked
-- computations that can be applied to data at runtime.
module Sara.DataFrame.Expression (
    -- * The Expression GADT
    Expr(..),
    -- * Evaluation
    evaluateExpr,
    -- * Smart Constructors
    lit,
    col,
    -- * Operators
    (+.+),
    (-.-),
    (*.*),
    (/.!),
    (===.),
    (>.),
    (<.),
    (>=.),
    (<=.),
    (&&.),
    (||.)
) where

import qualified Data.Text as T
import Data.Proxy (Proxy(..))
import GHC.TypeLits (Symbol, symbolVal)
import Data.Kind (Type)
import Sara.DataFrame.Types
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V



-- | A type-safe expression GADT for `DataFrame` operations.
-- The @cols@ parameter is the schema of the `DataFrame`, and @a@ is the return type of the expression.
-- The @dfMap@ parameter is the internal representation of the `DataFrame`.
-- The @idx@ parameter is the row index.
data Expr (cols :: [(Symbol, Type)]) a where
    -- | Represents a literal value in an expression.
    Lit :: CanBeDFValue a => a -> Expr cols a

    -- | Represents a column reference in an expression.
    Col :: (HasColumn c cols, CanBeDFValue a, TypeOf c cols ~ a) => Proxy c -> Expr cols a

    -- | Adds two numeric expressions.
    Add :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
    -- | Subtracts two numeric expressions.
    Subtract :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
    -- | Multiplies two numeric expressions.
    Multiply :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
    -- | Divides two numeric expressions.
    Divide :: (Fractional a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a

    -- | Checks if two expressions are equal.
    EqualTo :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
    -- | Checks if the first expression is greater than the second.
    GreaterThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
    -- | Checks if the first expression is less than the second.
    LessThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
    -- | Checks if the first expression is greater than or equal to the second.
    GreaterThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
    -- | Checks if the first expression is less than or equal to the second.
    LessThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool

    -- | Performs a logical AND on two boolean expressions.
    And :: Expr cols Bool -> Expr cols Bool -> Expr cols Bool
    -- | Performs a logical OR on two boolean expressions.
    Or :: Expr cols Bool -> Expr cols Bool -> Expr cols Bool
    -- | Applies a function to the result of an expression.
    ApplyFn :: (CanBeDFValue a, CanBeDFValue b) => (a -> b) -> Expr cols a -> Expr cols b

-- | Evaluates a type-safe expression on a given `DataFrame`'s internal map and row index.
-- Returns `Nothing` if any part of the expression evaluation fails (e.g., a column is missing, a value is `NA`).
evaluateExpr :: Expr cols a -> Map.Map T.Text Column -> Int -> Maybe a
evaluateExpr (Lit val) _ _ = Just val
evaluateExpr (Col p) dfMap idx = 
    case Map.lookup (T.pack (symbolVal p)) dfMap of
        Just colData -> if isNA (colData V.! idx) then Nothing else fromDFValue (colData V.! idx)
        Nothing -> Nothing
evaluateExpr (Add e1 e2) dfMap idx = liftA2 (+) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (Subtract e1 e2) dfMap idx = liftA2 (-) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (Multiply e1 e2) dfMap idx = liftA2 (*) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (Divide e1 e2) dfMap idx = liftA2 (/) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (EqualTo e1 e2) dfMap idx = liftA2 (==) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (GreaterThan e1 e2) dfMap idx = liftA2 (>) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (LessThan e1 e2) dfMap idx = liftA2 (<) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (GreaterThanOrEqualTo e1 e2) dfMap idx = liftA2 (>=) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (LessThanOrEqualTo e1 e2) dfMap idx = liftA2 (<=) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (And e1 e2) dfMap idx = liftA2 (&&) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (Or e1 e2) dfMap idx = liftA2 (||) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluateExpr (ApplyFn f expr) dfMap idx = f <$> evaluateExpr expr dfMap idx

-- | Smart constructor for a literal value expression.
lit :: CanBeDFValue a => a -> Expr cols a
lit = Lit

-- | Smart constructor for a column reference expression.
    -- | Smart constructor for a column reference expression.
col :: (HasColumn c cols, CanBeDFValue a, TypeOf c cols ~ a) => Proxy c -> Expr cols a
col = Col

-- | Infix operator for adding two numeric expressions.
(+.+) :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
(+.+) = Add

-- | Infix operator for subtracting two numeric expressions.
(-.-) :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
(-.-) = Subtract

-- | Infix operator for multiplying two numeric expressions.
(*.*) :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
(*.*) = Multiply

-- | Infix operator for dividing two numeric expressions.
(/.!) :: (Fractional a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
(/.!) = Divide

-- | Infix operator for checking equality between two expressions.
(===.) :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(===.) = EqualTo

-- | Infix operator for checking if the first expression is greater than the second.
(>.) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(>.) = GreaterThan

-- | Infix operator for checking if the first expression is less than the second.
(<.) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(<.) = LessThan

-- | Infix operator for checking if the first expression is greater than or equal to the second.
(>=.) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(>=.) = GreaterThanOrEqualTo

-- | Infix operator for checking if the first expression is less than or equal to the second.
(<=.) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(<=.) = LessThanOrEqualTo

-- | Infix operator for logical AND.
(&&.) :: Expr cols Bool -> Expr cols Bool -> Expr cols Bool
(&&.) = And

-- | Infix operator for logical OR.
(||.) :: Expr cols Bool -> Expr cols Bool -> Expr cols Bool
(||.) = Or