{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE StandaloneDeriving #-}

module ShouldNotCompile.SortNonOrdColumn where

import Sara.DataFrame.Types
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

-- Define a custom data type without an Ord instance
data MyCustomType = MyCustomType Int

deriving instance Show MyCustomType
deriving instance Eq MyCustomType

instance CanBeDFValue MyCustomType where
    toDFValue (MyCustomType i) = IntValue i -- For simplicity, map to IntValue
    fromDFValue (IntValue i) = Just (MyCustomType i)
    fromDFValue _ = Nothing

-- This should fail to compile because 'MyCustomColumn' has MyCustomType,
-- which does not have an Ord instance.

exampleDataFrame :: DataFrame '[ '("ColA", Int), '("MyCustomColumn", MyCustomType)]
exampleDataFrame = DataFrame Map.empty

-- Attempt to sort by MyCustomColumn, which should fail to compile
sortedDataFrame :: DataFrame '[ '("ColA", Int), '("MyCustomColumn", MyCustomType)]
sortedDataFrame = sortDataFrame [SortCriterion (Proxy @"MyCustomColumn") Ascending] exampleDataFrame
