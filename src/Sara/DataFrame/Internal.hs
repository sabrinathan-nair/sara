{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators #-}

-- | This module provides internal utilities for converting between Haskell records
-- and `DataFrame`s. It uses GHC.Generics to automatically derive the necessary
-- conversions.
module Sara.DataFrame.Internal where

import Data.Csv (FromNamedRecord)
import qualified Data.Vector as V
import Sara.DataFrame.Types
import qualified Data.Map.Strict as Map
import Data.Proxy (Proxy(..))
import GHC.TypeLits (Symbol)
import Data.Kind (Type)
import GHC.Generics
import qualified Data.Text as T ()
import qualified Data.Text.Encoding as TE ()
import Data.Aeson ()
import qualified Data.Aeson.Types as A ()

-- | Converts a record to a list of Text values.
recordToDFValueList :: (Generic a, GToFields (Rep a)) => a -> [DFValue]
recordToDFValueList = gtoFields . from

-- | A typeclass for records that can be converted to a `DataFrame`.
-- It uses `DefaultSignatures` to provide a default implementation for any
-- type that has a `Generic` instance.
class KnownColumns (Schema a) => ToDataFrameRecord a where
  -- | Converts a `Vector` of records into a `DataFrame`.
  toDataFrame :: V.Vector a -> DataFrame (Schema a)
  default toDataFrame :: (Generic a, GToFields (Rep a)) => V.Vector a -> DataFrame (Schema a)
  toDataFrame vec = DataFrame $ Map.fromList $ zip (columnNames (Proxy :: Proxy (Schema a))) (map V.fromList (transpose (map (gtoFields . from) (V.toList vec))))

instance (Generic a, GToFields (Rep a), FromNamedRecord a, KnownColumns (Schema a)) => ToDataFrameRecord a

-- | A typeclass that associates a record type with its `DataFrame` schema.
class HasSchema a where
  -- | The schema of the record, represented as a type-level list of `(Symbol, Type)`.
  type Schema a :: [(Symbol, Type)]

-- | A generic typeclass for converting a generic representation of a record
-- into a list of `DFValue`s.
class GToFields f where
  -- | Converts a generic representation of a record into a list of `DFValue`s.
  gtoFields :: f a -> [DFValue]

instance GToFields U1 where
  gtoFields U1 = []

instance (GToFields a, GToFields b) => GToFields (a :*: b) where
  gtoFields (a :*: b) = gtoFields a ++ gtoFields b

instance (GToFields a) => GToFields (M1 i c a) where
  gtoFields (M1 a) = gtoFields a

instance (CanBeDFValue a) => GToFields (K1 i a) where
  gtoFields (K1 a) = [toDFValue a]

-- | Transposes a list of lists.
-- Assumes that all inner lists have the same length.
transpose :: [[a]] -> [[a]]
transpose ([]:_) = []
transpose x = map head x : transpose (map tail x)