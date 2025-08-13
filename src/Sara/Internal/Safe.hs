{-# LANGUAGE Safe #-}

-- | Safe utility functions to avoid partial operations.
--   This module wraps potentially unsafe Prelude functions
--   and provides total (safe) alternatives.
module Sara.Internal.Safe
  ( safeHead
  , safeTail
  , safeInit
  , safeLast
  , safeMaximum
  , safeMinimum
  , safeLookup
  , safeIndex
  , safeRead
  ) where

import qualified Data.Maybe as Maybe
import qualified Text.Read as Read

-- | Safe version of 'head'
safeHead :: [a] -> Maybe a
safeHead []    = Nothing
safeHead (x:_) = Just x

-- | Safe version of 'tail'
safeTail :: [a] -> Maybe [a]
safeTail []     = Nothing
safeTail (_:xs) = Just xs

-- | Safe version of 'init'
safeInit :: [a] -> Maybe [a]
safeInit [] = Nothing
safeInit [_] = Just []
safeInit (x:xs) = fmap (x:) (safeInit xs)

-- | Safe version of 'last'
safeLast :: [a] -> Maybe a
safeLast [] = Nothing
safeLast [x] = Just x
safeLast (_:xs) = safeLast xs

-- | Safe version of 'maximum'
safeMaximum :: Ord a => [a] -> Maybe a
safeMaximum [] = Nothing
safeMaximum (x:xs) = Just (foldr max x xs)

-- | Safe version of 'minimum'
safeMinimum :: Ord a => [a] -> Maybe a
safeMinimum [] = Nothing
safeMinimum (x:xs) = Just (foldr min x xs)

-- | Safe lookup for lists of pairs or Maps
safeLookup :: (Eq k, Ord k) => k -> [(k, v)] -> Maybe v
safeLookup _ [] = Nothing
safeLookup k xs = Maybe.listToMaybe [v | (k', v) <- xs, k == k']

-- | Safe index operation
safeIndex :: [a] -> Int -> Maybe a
safeIndex xs i
  | i < 0 = Nothing
  | otherwise = case (xs, i) of
      ([], _) -> Nothing
      (x:_, 0) -> Just x
      (_:xs', n) -> safeIndex xs' (n - 1)

-- | Safe read function
safeRead :: Read a => String -> Maybe a
safeRead = Read.readMaybe
