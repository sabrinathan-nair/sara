{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

module Sara.DataFrame.TimeSeries (
    resample,
    ResampleRule(..),
    shift,
    pctChange,
    fromRows,
    groupByTime,
    rollingApply
) where

import Sara.DataFrame.Types (DataFrame(..), Row, DFValue(..), KnownColumns(..), toRows, fromRows, HasColumn, TypeOf, CanAggregate(..), CanBeDFValue(..))
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Data.Time
import Data.List (foldl')
import Data.Proxy (Proxy(..))
import GHC.TypeLits (Symbol, KnownSymbol, symbolVal)