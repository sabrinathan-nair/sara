{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

module Sara.DataFrame.Strings (
    lower,
    upper,
    strip,
    contains,
    replace
) where

import Sara.DataFrame.Types (DataFrame(..), DFValue(..), KnownColumns(..), HasColumn)
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import GHC.TypeLits (Symbol, KnownSymbol, symbolVal)
import Data.Proxy (Proxy(..))

-- Helper to apply a Text transformation function to a DFValue, handling non-TextValue and NA
applyTextTransform :: (T.Text -> T.Text) -> DFValue -> DFValue
applyTextTransform f (TextValue t) = TextValue (f t)
applyTextTransform _ NA = NA
applyTextTransform _ other = other -- Return other types as is

-- Helper to apply a Text predicate function to a DFValue, returning BoolValue
applyTextPredicate :: (T.Text -> Bool) -> DFValue -> DFValue
applyTextPredicate p (TextValue t) = BoolValue (p t)
applyTextPredicate _ NA = NA
applyTextPredicate _ other = other -- Return other types as is (or maybe NA? Pandas returns NA for non-string types)

-- | Converts all characters in the specified column to lowercase.
lower :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> DataFrame cols -> DataFrame cols
lower colProxy (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.adjust (V.map (applyTextTransform T.toLower)) colName dfMap

-- | Converts all characters in the specified column to uppercase.
upper :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> DataFrame cols -> DataFrame cols
upper colProxy (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.adjust (V.map (applyTextTransform T.toUpper)) colName dfMap

-- | Removes leading and trailing whitespace from strings in the specified column.
strip :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> DataFrame cols -> DataFrame cols
strip colProxy (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.adjust (V.map (applyTextTransform T.strip)) colName dfMap

-- | Checks if strings in the specified column contain a given pattern.
contains :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> T.Text -> DataFrame cols -> DataFrame cols
contains colProxy pattern (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.insert (colName `T.append` "_contains_" `T.append` pattern) (V.map (applyTextPredicate (T.isInfixOf pattern)) (dfMap Map.! colName)) dfMap

-- | Replaces occurrences of a substring in the specified column.
replace :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> T.Text -> T.Text -> DataFrame cols -> DataFrame cols
replace colProxy old new (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.adjust (V.map (applyTextTransform (T.replace old new))) colName dfMap