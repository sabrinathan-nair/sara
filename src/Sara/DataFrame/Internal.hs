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

module Sara.DataFrame.Internal where

import Data.Csv (FromNamedRecord)
import qualified Data.Vector as V
import Sara.DataFrame.Types
import qualified Data.Map.Strict as Map
import Data.Proxy (Proxy(..))
import GHC.TypeLits (Symbol)
import Data.Kind (Type)
import GHC.Generics

class KnownColumns (Schema a) => ToDataFrameRecord a where
  toDataFrame :: V.Vector a -> DataFrame (Schema a)
  default toDataFrame :: (Generic a, GToFields (Rep a)) => V.Vector a -> DataFrame (Schema a)
  toDataFrame vec = DataFrame $ Map.fromList $ zip (columnNames (Proxy :: Proxy (Schema a))) (map V.fromList (transpose (map (gtoFields . from) (V.toList vec))))

instance (Generic a, GToFields (Rep a), FromNamedRecord a, KnownColumns (Schema a)) => ToDataFrameRecord a

class HasSchema a where
  type Schema a :: [(Symbol, Type)]

class GToFields f where
  gtoFields :: f a -> [DFValue]

instance GToFields U1 where
  gtoFields U1 = []

instance (GToFields a, GToFields b) => GToFields (a :*: b) where
  gtoFields (a :*: b) = gtoFields a ++ gtoFields b

instance (GToFields a) => GToFields (M1 i c a) where
  gtoFields (M1 a) = gtoFields a

instance (CanBeDFValue a) => GToFields (K1 i a) where
  gtoFields (K1 a) = [toDFValue a]

transpose :: [[a]] -> [[a]]
transpose ([]:_) = []
transpose x = (map head x) : transpose (map tail x)