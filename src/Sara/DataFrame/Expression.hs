{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE KindSignatures #-}

module Sara.DataFrame.Expression (
    Expr,
    evaluateExpr,
    lit,
    col,
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
import GHC.TypeLits (Symbol, KnownSymbol, symbolVal)
import Sara.DataFrame.Types
import qualified Data.Map.Strict as Map

-- | A type-safe expression GADT for DataFrame operations.
-- 'cols' is the schema of the DataFrame, and 'a' is the return type of the expression.
data Expr (cols :: [Symbol]) a where
    -- Literals
    Lit :: CanBeDFValue a => a -> Expr cols a

    -- Column reference
    Col :: (HasColumn c cols, CanBeDFValue a) => Proxy c -> Expr cols a

    -- Numeric operations
    Add :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
    Subtract :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
    Multiply :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
    Divide :: (Fractional a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a

    -- Comparison operations
    EqualTo :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
    GreaterThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
    LessThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
    GreaterThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
    LessThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool

    -- Boolean operations
    And :: Expr cols Bool -> Expr cols Bool -> Expr cols Bool
    Or :: Expr cols Bool -> Expr cols Bool -> Expr cols Bool

-- | Evaluates a type-safe expression on a given row.
-- Returns Nothing if any column lookup or type conversion fails.
evaluateExpr :: Expr cols a -> Row -> Maybe a
evaluateExpr (Lit val) _ = Just val
evaluateExpr (Col p) row = do
    let colName = T.pack (symbolVal p)
    dfValue <- Map.lookup colName row
    fromDFValue dfValue
evaluateExpr (Add e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 + v2)
evaluateExpr (Subtract e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 - v2)
evaluateExpr (Multiply e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 * v2)
evaluateExpr (Divide e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 / v2)
evaluateExpr (EqualTo e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 == v2)
evaluateExpr (GreaterThan e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 > v2)
evaluateExpr (LessThan e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 < v2)
evaluateExpr (GreaterThanOrEqualTo e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 >= v2)
evaluateExpr (LessThanOrEqualTo e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 <= v2)
evaluateExpr (And e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 && v2)
evaluateExpr (Or e1 e2) row = do
    v1 <- evaluateExpr e1 row
    v2 <- evaluateExpr e2 row
    return (v1 || v2)

-- | Smart constructor for a literal value.
lit :: CanBeDFValue a => a -> Expr cols a
lit = Lit

-- | Smart constructor for a column reference.
col :: (HasColumn c cols, CanBeDFValue a) => Proxy c -> Expr cols a
col = Col

-- | Infix operators for expressions.
(+.+) :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
(+.+) = Add

(-.-) :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
(-.-) = Subtract

(*.*) :: (Num a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
(*.*) = Multiply

(/.!) :: (Fractional a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols a
(/.!) = Divide

(===.) :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(===.) = EqualTo

(>.) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(>.) = GreaterThan

(<.) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(<.) = LessThan

(>=.) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(>=.) = GreaterThanOrEqualTo

(<=.) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Expr cols Bool
(<=.) = LessThanOrEqualTo

(&&.) :: Expr cols Bool -> Expr cols Bool -> Expr cols Bool
(&&.) = And

(||.) :: Expr cols Bool -> Expr cols Bool -> Expr cols Bool
(||.) = Or
