{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators #-}

-- | This module provides functions for string manipulation on `DataFrame` columns.
-- These functions operate on columns of type `TextValue` and handle non-text values gracefully.
module Sara.DataFrame.Strings (
    -- * String Transformations
    lower,
    upper,
    strip,
    -- * String Predicates
    contains,
    -- * String Replacements
    replace
) where

import Sara.DataFrame.Types (DataFrame(..), DFValue(..), KnownColumns(..), HasColumn, TypeOf)
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import GHC.TypeLits (Symbol, KnownSymbol, symbolVal)
import Data.Proxy (Proxy(..))

-- | Applies a `Text` transformation function to a `DFValue`.
-- Non-`TextValue`s and `NA` are returned unchanged.
applyTextTransform :: (T.Text -> T.Text) -> DFValue -> DFValue
applyTextTransform f (TextValue t) = TextValue (f t)
applyTextTransform _ NA = NA
applyTextTransform _ other = other -- Return other types as is

-- | Applies a `Text` predicate function to a `DFValue`, returning a `BoolValue`.
-- Non-`TextValue`s and `NA` are returned unchanged.
applyTextPredicate :: (T.Text -> Bool) -> DFValue -> DFValue
applyTextPredicate p (TextValue t) = BoolValue (p t)
applyTextPredicate _ NA = NA
applyTextPredicate _ other = other -- Return other types as is (or maybe NA? Pandas returns NA for non-string types)

-- | Converts all characters in the specified column to lowercase.
-- The column must be of type `TextValue`.
lower :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> DataFrame cols -> DataFrame cols
lower colProxy (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.adjust (V.map (applyTextTransform T.toLower)) colName dfMap

-- | Converts all characters in the specified column to uppercase.
-- The column must be of type `TextValue`.
upper :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> DataFrame cols -> DataFrame cols
upper colProxy (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.adjust (V.map (applyTextTransform T.toUpper)) colName dfMap

-- | Removes leading and trailing whitespace from strings in the specified column.
-- The column must be of type `TextValue`.
strip :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> DataFrame cols -> DataFrame cols
strip colProxy (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.adjust (V.map (applyTextTransform T.strip)) colName dfMap

contains :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols) => Proxy col -> T.Text -> DataFrame cols -> DataFrame cols
-- | Checks if strings in the specified column contain a given pattern.
-- A new column is added to the `DataFrame` with the results.
-- The new column is named by appending "_contains_" and the pattern to the original column name.
contains colProxy pattern (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.insert (colName `T.append` T.pack "_contains_" `T.append` pattern) (V.map (applyTextPredicate (T.isInfixOf pattern)) (dfMap Map.! colName)) dfMap

-- | Replaces occurrences of a substring in the specified column.
-- The column must be of type `TextValue`.
replace :: forall (col :: Symbol) cols. (KnownSymbol col, HasColumn col cols, KnownColumns cols, TypeOf col cols ~ T.Text) => Proxy col -> T.Text -> T.Text -> DataFrame cols -> DataFrame cols
replace colProxy old new (DataFrame dfMap) =
    let colName = T.pack (symbolVal colProxy)
    in DataFrame $ Map.adjust (V.map (applyTextTransform (T.replace old new))) colName dfMap