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

import qualified Data.List as List
import qualified Data.Maybe as Maybe
import qualified Text.Read as Read
import qualified Data.Map.Strict as Map

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
safeInit xs = Just (List.init xs)

-- | Safe version of 'last'
safeLast :: [a] -> Maybe a
safeLast [] = Nothing
safeLast xs = Just (List.last xs)

-- | Safe version of 'maximum'
safeMaximum :: Ord a => [a] -> Maybe a
safeMaximum [] = Nothing
safeMaximum xs = Just (List.maximum xs)

-- | Safe version of 'minimum'
safeMinimum :: Ord a => [a] -> Maybe a
safeMinimum [] = Nothing
safeMinimum xs = Just (List.minimum xs)

-- | Safe lookup for lists of pairs or Maps
safeLookup :: (Eq k, Ord k) => k -> [(k, v)] -> Maybe v
safeLookup _ [] = Nothing
safeLookup k xs = Maybe.listToMaybe [v | (k', v) <- xs, k == k']

-- | Safe index operation
safeIndex :: [a] -> Int -> Maybe a
safeIndex xs i
  | i < 0 || i >= length xs = Nothing
  | otherwise               = Just (xs !! i)

-- | Safe read function
safeRead :: Read a => String -> Maybe a
safeRead = Read.readMaybe
