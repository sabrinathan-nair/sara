{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

-- | This module defines a type-safe GADT for building predicates to filter `DataFrame` rows.
-- It leverages the `Expr` GADT to create complex, type-checked conditions.
module Sara.DataFrame.Predicate (
    -- * The Predicate GADT
    Predicate,
    -- * Evaluation
    evaluate,
    -- * Logical Operators
    (&&&),
    (|||),
    -- * Comparison Operators
    (>.>),
    (<.<),
    (===),
    (>=.>),
    (<=.<)
) where

import Data.Kind (Type)
import Sara.DataFrame.Types
import GHC.TypeLits
import Sara.DataFrame.Expression (Expr, evaluateExpr)
import Control.Applicative (liftA2)

-- | A type-safe predicate for filtering a `DataFrame`.
-- The @cols@ type parameter ensures that the predicate only refers to columns
-- that are actually in the `DataFrame`.
data Predicate (cols :: [(Symbol, Type)]) where
    -- | Combines two predicates with a logical AND.
    And :: Predicate cols -> Predicate cols -> Predicate cols
    -- | Combines two predicates with a logical OR.
    Or :: Predicate cols -> Predicate cols -> Predicate cols
    -- | Checks if the first expression is greater than the second.
    GreaterThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
    -- | Checks if the first expression is less than the second.
    LessThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
    -- | Checks if two expressions are equal.
    EqualTo :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
    -- | Checks if the first expression is greater than or equal to the second.
    GreaterThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
    -- | Checks if the first expression is less than or equal to the second.
    LessThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols

-- | Evaluates a `Predicate` on a given `Row`.
-- Returns `Nothing` if the predicate cannot be evaluated (e.g., due to `NA` values).
--
-- >>> :set -XDataKinds
-- >>> import qualified Data.Map.Strict as Map
-- >>> let row = Map.fromList [("age", IntValue 30), ("salary", DoubleValue 50000.0)]
-- >>> let predicate = col @"age" >.> lit 25 &&& col @"salary" <.< lit 60000.0
-- >>> evaluate predicate row
-- Just True
evaluate :: Predicate cols -> Row -> Maybe Bool
evaluate (And p1 p2) row = liftA2 (&&) (evaluate p1 row) (evaluate p2 row)
evaluate (Or p1 p2) row = liftA2 (||) (evaluate p1 row) (evaluate p2 row)
evaluate (GreaterThan e1 e2) row = liftA2 (>) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluate (LessThan e1 e2) row = liftA2 (<) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluate (EqualTo e1 e2) row = liftA2 (==) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluate (GreaterThanOrEqualTo e1 e2) row = liftA2 (>=) (evaluateExpr e1 row) (evaluateExpr e2 row)
evaluate (LessThanOrEqualTo e1 e2) row = liftA2 (<=) (evaluateExpr e1 row) (evaluateExpr e2 row)

-- | Infix operator for logical AND.
(&&&) :: Predicate cols -> Predicate cols -> Predicate cols
(&&&) = And

-- | Infix operator for logical OR.
(|||) :: Predicate cols -> Predicate cols -> Predicate cols
(|||) = Or

-- | Infix operator for greater than.
(>.>) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
(>.>) = GreaterThan

-- | Infix operator for less than.
(<.<) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
(<.<) = LessThan

-- | Infix operator for equal to.
(===) :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
(===) = EqualTo

-- | Infix operator for greater than or equal to.
(>=.>) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
(>=.>) = GreaterThanOrEqualTo

-- | Infix operator for less than or equal to.
(<=.<) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
(<=.<) = LessThanOrEqualTo