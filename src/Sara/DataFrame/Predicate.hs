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
    FilterPredicate(..),
    RowPredicate(..),
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

import qualified Data.Map.Strict as Map
import qualified Data.Text as T

-- | A type-safe predicate for filtering a `DataFrame`.
-- The @cols@ type parameter ensures that the predicate only refers to columns
-- that are actually in the `DataFrame`.
data FilterPredicate (cols :: [(Symbol, Type)]) where
    FilterPredicate :: RowPredicate cols -> FilterPredicate cols

data RowPredicate (cols :: [(Symbol, Type)]) where
    -- | Combines two predicates with a logical AND.
    And :: RowPredicate cols -> RowPredicate cols -> RowPredicate cols
    -- | Combines two predicates with a logical OR.
    Or :: RowPredicate cols -> RowPredicate cols -> RowPredicate cols
    -- | Checks if the first expression is greater than the second.
    GreaterThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> RowPredicate cols
    -- | Checks if the first expression is less than the second.
    LessThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> RowPredicate cols
    -- | Checks if two expressions are equal.
    EqualTo :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> RowPredicate cols
    -- | Checks if the first expression is greater than or equal to the second.
    GreaterThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> RowPredicate cols
    -- | Checks if the first expression is less than or equal to the second.
    LessThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> RowPredicate cols
    -- | Wraps an expression as a predicate.
    ExprPredicate :: Expr cols Bool -> RowPredicate cols

-- | Evaluates a `Predicate` on a given `DataFrame`'s internal map and row index.
-- Returns `Nothing` if the predicate cannot be evaluated (e.g., due to `NA` values).
--
-- >>> :set -XDataKinds
-- >>> import qualified Data.Map.Strict as Map
-- >>> let row = Map.fromList [("age", IntValue 30), ("salary", DoubleValue 50000.0)]
-- >>> let predicate = col @"age" >.> lit 25 &&& col @"salary" <.< lit 60000.0
-- >>> evaluate predicate row
-- Just True
evaluate :: FilterPredicate cols -> Map.Map T.Text Column -> Int -> Maybe Bool
evaluate (FilterPredicate p) dfMap idx = evaluatePredicate p dfMap idx

-- | Evaluates a `RowPredicate` on a given `DataFrame`'s internal map and row index.
evaluatePredicate :: RowPredicate cols -> Map.Map T.Text Column -> Int -> Maybe Bool
evaluatePredicate (And p1 p2) dfMap idx = liftA2 (&&) (evaluatePredicate p1 dfMap idx) (evaluatePredicate p2 dfMap idx)
evaluatePredicate (Or p1 p2) dfMap idx = liftA2 (||) (evaluatePredicate p1 dfMap idx) (evaluatePredicate p2 dfMap idx)
evaluatePredicate (GreaterThan e1 e2) dfMap idx = liftA2 (>) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluatePredicate (LessThan e1 e2) dfMap idx = liftA2 (<) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluatePredicate (EqualTo e1 e2) dfMap idx = liftA2 (==) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluatePredicate (GreaterThanOrEqualTo e1 e2) dfMap idx = liftA2 (>=) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluatePredicate (LessThanOrEqualTo e1 e2) dfMap idx = liftA2 (<=) (evaluateExpr e1 dfMap idx) (evaluateExpr e2 dfMap idx)
evaluatePredicate (ExprPredicate e) dfMap idx = evaluateExpr e dfMap idx

-- | Infix operator for logical AND.
(&&&) :: FilterPredicate cols -> FilterPredicate cols -> FilterPredicate cols
(FilterPredicate p1) &&& (FilterPredicate p2) = FilterPredicate (And p1 p2)

-- | Infix operator for logical OR.
(|||) :: FilterPredicate cols -> FilterPredicate cols -> FilterPredicate cols
(FilterPredicate p1) ||| (FilterPredicate p2) = FilterPredicate (Or p1 p2)

-- | Infix operator for greater than.
(>.>) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> FilterPredicate cols
e1 >.> e2 = FilterPredicate (GreaterThan e1 e2)

-- | Infix operator for less than.
(<.<) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> FilterPredicate cols
e1 <.< e2 = FilterPredicate (LessThan e1 e2)

-- | Infix operator for equal to.
(===) :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> FilterPredicate cols
e1 === e2 = FilterPredicate (EqualTo e1 e2)

-- | Infix operator for greater than or equal to.
(>=.>) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> FilterPredicate cols
e1 >=.> e2 = FilterPredicate (GreaterThanOrEqualTo e1 e2)

-- | Infix operator for less than or equal to.
(<=.<) :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> FilterPredicate cols
e1 <=.< e2 = FilterPredicate (LessThanOrEqualTo e1 e2)