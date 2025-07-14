{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE KindSignatures #-}

module Sara.DataFrame.Expression (
    Expr(..),
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
import GHC.TypeLits (Symbol, symbolVal)
import Data.Kind (Type)
import Sara.DataFrame.Types
import qualified Data.Map.Strict as Map

-- | A type-safe expression GADT for DataFrame operations.
-- 'cols' is the schema of the DataFrame, and 'a' is the return type of the expression.
data Expr (cols :: [(Symbol, Type)]) a where
    -- Literals
    Lit :: CanBeDFValue a => a -> Expr cols a

    -- Column reference
    Col :: (HasColumn c cols, CanBeDFValue a, TypeOf c cols ~ a) => Proxy c -> Expr cols a

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
    ApplyFn :: (CanBeDFValue a, CanBeDFValue b) => (a -> b) -> Expr cols a -> Expr cols b

-- | Evaluates a type-safe expression on a given row.
evaluateExpr :: Expr cols a -> Row -> Maybe a
evaluateExpr (Lit val) _ = Just val
evaluateExpr (Col p) row = fromDFValue (Map.findWithDefault NA (T.pack (symbolVal p)) row)
evaluateExpr (Add e1 e2) row = liftA2 (+) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (Subtract e1 e2) row = liftA2 (-) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (Multiply e1 e2) row = liftA2 (*) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (Divide e1 e2) row = liftA2 (/) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (EqualTo e1 e2) row = liftA2 (==) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (GreaterThan e1 e2) row = liftA2 (>) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (LessThan e1 e2) row = liftA2 (<) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (GreaterThanOrEqualTo e1 e2) row = liftA2 (>=) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (LessThanOrEqualTo e1 e2) row = liftA2 (<=) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (And e1 e2) row = liftA2 (&&) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (Or e1 e2) row = liftA2 (||) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluateExpr (ApplyFn f expr) row = f <$> evaluateExpr expr row

-- | Smart constructor for a literal value.
lit :: CanBeDFValue a => a -> Expr cols a
lit = Lit

-- | Smart constructor for a column reference.
col :: (HasColumn c cols, CanBeDFValue a, TypeOf c cols ~ a) => Proxy c -> Expr cols a
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
