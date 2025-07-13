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

module Sara.DataFrame.Predicate (
    Predicate,
    evaluate,
    (&&&),
    (|||),
    (>.>),
    (<.<),
    (===),
    (>=.>),
    (<=.<)
) where

import Data.Kind (Type)
import Sara.DataFrame.Types
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)

import Sara.DataFrame.Expression (Expr, evaluateExpr)

-- | A type-safe predicate for filtering a DataFrame.
-- The 'cols' type parameter ensures that the predicate only refers to columns
-- that are actually in the DataFrame.
data Predicate (cols :: [(Symbol, Type)]) where
    And :: Predicate cols -> Predicate cols -> Predicate cols
    Or :: Predicate cols -> Predicate cols -> Predicate cols
    GreaterThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
    LessThan :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
    EqualTo :: (Eq a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
    GreaterThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols
    LessThanOrEqualTo :: (Ord a, CanBeDFValue a) => Expr cols a -> Expr cols a -> Predicate cols

-- | A helper function to evaluate a predicate on a given row.
evaluate :: Predicate cols -> Row -> Bool
evaluate (And p1 p2) row = evaluate p1 row && evaluate p2 row
evaluate (Or p1 p2) row = evaluate p1 row || evaluate p2 row
evaluate (GreaterThan e1 e2) row = evaluateExpr e1 row > evaluateExpr e2 row
evaluate (LessThan e1 e2) row = evaluateExpr e1 row < evaluateExpr e2 row
evaluate (EqualTo e1 e2) row = evaluateExpr e1 row == evaluateExpr e2 row
evaluate (GreaterThanOrEqualTo e1 e2) row = evaluateExpr e1 row >= evaluateExpr e2 row
evaluate (LessThanOrEqualTo e1 e2) row = evaluateExpr e1 row <= evaluateExpr e2 row

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