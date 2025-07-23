{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Sara.DataFrame.Apply (
    Apply(..),
    CanApply(..),
    GetCols,
    GetNewCols
) where

import Data.Kind (Type)
import Sara.DataFrame.Types
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V

data Apply (cols :: [(Symbol, Type)]) (newCols :: [(Symbol, Type)]) where
  Apply :: (HasColumn col cols, KnownColumns cols, CanBeDFValue oldType, CanBeDFValue newType, TypeOf col cols ~ oldType, newCols ~ UpdateColumn col newType cols, KnownColumns newCols)
        => Proxy col
        -> (oldType -> newType)
        -> Apply cols newCols

class CanApply f where
  apply :: f -> DataFrame (GetCols f) -> DataFrame (GetNewCols f)

instance CanApply (Apply cols newCols) where
  apply (Apply (colProxy :: Proxy col) f) (DataFrame dfMap) =
    let
        colName = T.pack (symbolVal colProxy)
        transformDFValue :: DFValue -> DFValue
        transformDFValue dfVal = case fromDFValue @(TypeOf col cols) dfVal of
            Just val -> toDFValue (f val)
            Nothing  -> NA
        updatedCol = V.map transformDFValue (dfMap Map.! colName)
    in DataFrame (Map.insert colName updatedCol dfMap)

type family GetCols (f :: k) :: [(Symbol, Type)] where
  GetCols (Apply cols _) = cols

type family GetNewCols (f :: k) :: [(Symbol, Type)] where
  GetNewCols (Apply _ newCols) = newCols


