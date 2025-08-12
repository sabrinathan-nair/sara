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
import Streaming (Stream, Of)
import qualified Streaming.Prelude as S


import Sara.Error (SaraError(..))

data Apply (cols :: [(Symbol, Type)]) (newCols :: [(Symbol, Type)]) where
  Apply :: (HasColumn col cols, KnownColumns cols, CanBeDFValue oldType, CanBeDFValue newType, TypeOf col cols ~ oldType, newCols ~ UpdateColumn col newType cols, KnownColumns newCols)
        => Proxy col
        -> (oldType -> newType)
        -> Apply cols newCols

class CanApply f where
  apply :: f -> Stream (Of (DataFrame (GetCols f))) IO () -> Stream (Of (DataFrame (GetNewCols f))) IO ()

instance CanApply (Apply cols newCols) where
  apply (Apply (colProxy :: Proxy col) f) dfStream = S.mapM (\df -> do
    let colName = T.pack (symbolVal colProxy)
    let transformDFValue :: DFValue -> Either SaraError DFValue
        transformDFValue dfVal = case fromDFValue @(TypeOf col cols) dfVal of
            Right val -> Right $ toDFValue (f val)
            Left err  -> Left err
    
    case Map.lookup colName (getDataFrameMap df) of
        Just column -> case traverse transformDFValue column of
            Right updatedCol -> return $ DataFrame (Map.insert colName updatedCol (getDataFrameMap df))
            Left _ -> return $ fromRows [] -- Yield empty DataFrame on error
        Nothing -> return $ fromRows [] -- Yield empty DataFrame on error
    ) dfStream

type family GetCols (f :: k) :: [(Symbol, Type)] where
  GetCols (Apply cols _) = cols

type family GetNewCols (f :: k) :: [(Symbol, Type)] where
  GetNewCols (Apply _ newCols) = newCols


