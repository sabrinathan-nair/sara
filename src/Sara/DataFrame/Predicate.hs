
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

import Sara.DataFrame.Types
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import qualified Data.Text as T
import qualified Data.Map.Strict as Map

-- | A type-safe predicate for filtering a DataFrame.
-- The 'cols' type parameter ensures that the predicate only refers to columns
-- that are actually in the DataFrame.
data Predicate (cols :: [Symbol]) =
    And (Predicate cols) (Predicate cols) |
    Or (Predicate cols) (Predicate cols) |
    GreaterThan T.Text DFValue |
    LessThan T.Text DFValue |
    EqualTo T.Text DFValue |
    GreaterThanOrEqualTo T.Text DFValue |
    LessThanOrEqualTo T.Text DFValue

-- | A helper function to evaluate a predicate on a given row.
evaluate :: Predicate cols -> Row -> Bool
evaluate (And p1 p2) row = evaluate p1 row && evaluate p2 row
evaluate (Or p1 p2) row = evaluate p1 row || evaluate p2 row
evaluate (GreaterThan col val) row =
    case Map.lookup col row of
        Just v -> v > val
        Nothing -> False
evaluate (LessThan col val) row =
    case Map.lookup col row of
        Just v -> v < val
        Nothing -> False
evaluate (EqualTo col val) row =
    case Map.lookup col row of
        Just v -> v == val
        Nothing -> False
evaluate (GreaterThanOrEqualTo col val) row =
    case Map.lookup col row of
        Just v -> v >= val
        Nothing -> False
evaluate (LessThanOrEqualTo col val) row =
    case Map.lookup col row of
        Just v -> v <= val
        Nothing -> False

-- | Infix operator for logical AND.
(&&&) :: Predicate cols -> Predicate cols -> Predicate cols
(&&&) = And

-- | Infix operator for logical OR.
(|||) :: Predicate cols -> Predicate cols -> Predicate cols
(|||) = Or

-- | Infix operator for greater than.
(>.>) :: forall col cols. (KnownSymbol col, HasColumn col cols) => Proxy col -> DFValue -> Predicate cols
(>.>) _ val = GreaterThan (T.pack (symbolVal (Proxy @col))) val

-- | Infix operator for less than.
(<.<) :: forall col cols. (KnownSymbol col, HasColumn col cols) => Proxy col -> DFValue -> Predicate cols
(<.<) _ val = LessThan (T.pack (symbolVal (Proxy @col))) val

-- | Infix operator for equal to.
(===) :: forall col cols. (KnownSymbol col, HasColumn col cols) => Proxy col -> DFValue -> Predicate cols
(===) _ val = EqualTo (T.pack (symbolVal (Proxy @col))) val

-- | Infix operator for greater than or equal to.
(>=.>) :: forall col cols. (KnownSymbol col, HasColumn col cols) => Proxy col -> DFValue -> Predicate cols
(>=.>) _ val = GreaterThanOrEqualTo (T.pack (symbolVal (Proxy @col))) val

-- | Infix operator for less than or equal to.
(<=.<) :: forall col cols. (KnownSymbol col, HasColumn col cols) => Proxy col -> DFValue -> Predicate cols
(<=.<) _ val = LessThanOrEqualTo (T.pack (symbolVal (Proxy @col))) val
