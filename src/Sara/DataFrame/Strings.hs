{-# LANGUAGE OverloadedStrings #-}

module Sara.DataFrame.Strings (
    lower,
    upper,
    strip,
    contains,
    replace
) where

import Sara.DataFrame.Types
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

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
lower :: DataFrame -> T.Text -> DataFrame
lower (DataFrame dfMap) colName = DataFrame $ Map.adjust (V.map (applyTextTransform T.toLower)) colName dfMap

-- | Converts all characters in the specified column to uppercase.
upper :: DataFrame -> T.Text -> DataFrame
upper (DataFrame dfMap) colName = DataFrame $ Map.adjust (V.map (applyTextTransform T.toUpper)) colName dfMap

-- | Removes leading and trailing whitespace from strings in the specified column.
strip :: DataFrame -> T.Text -> DataFrame
strip (DataFrame dfMap) colName = DataFrame $ Map.adjust (V.map (applyTextTransform T.strip)) colName dfMap

-- | Checks if strings in the specified column contain a given pattern.
contains :: DataFrame -> T.Text -> T.Text -> DataFrame
contains (DataFrame dfMap) colName pattern = DataFrame $ Map.insert (colName `T.append` "_contains_" `T.append` pattern) (V.map (applyTextPredicate (T.isInfixOf pattern)) (dfMap Map.! colName)) dfMap

-- | Replaces occurrences of a substring in the specified column.
replace :: DataFrame -> T.Text -> T.Text -> T.Text -> DataFrame
replace (DataFrame dfMap) colName old new = DataFrame $ Map.adjust (V.map (applyTextTransform (T.replace old new))) colName dfMap
