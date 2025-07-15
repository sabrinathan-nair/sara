{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

-- | This module provides functions for joining `DataFrame`s.
module Sara.DataFrame.Join (
    joinDF
) where

import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Sara.DataFrame.Types
import Control.Applicative ((<|>))
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import Data.Kind (Type)

-- | Joins two `DataFrame`s based on a list of common columns and a `JoinType`.
-- The schema of the resulting `DataFrame` is inferred from the input schemas and the join type.
--
-- The `onCols` parameter specifies the columns to join on. These columns must exist in both `DataFrame`s.
--
-- The `joinType` parameter determines how to handle non-matching rows:
--
-- *   `InnerJoin`: Only rows with matching keys in both `DataFrame`s are kept.
-- *   `LeftJoin`: All rows from the left `DataFrame` are kept. If there is no match in the right `DataFrame`, the corresponding columns will be `NA`.
-- *   `RightJoin`: All rows from the right `DataFrame` are kept. If there is no match in the left `DataFrame`, the corresponding columns will be `NA`.
-- *   `OuterJoin`: All rows from both `DataFrame`s are kept. Where there is no match, the corresponding columns will be `NA`.
joinDF :: forall (onCols :: [(Symbol, Type)]) (cols1 :: [(Symbol, Type)]) (cols2 :: [(Symbol, Type)]) (joinType :: JoinType) (colsOut :: [(Symbol, Type)]).
          ( HasColumns (MapSymbols onCols) cols1, HasColumns (MapSymbols onCols) cols2
          , KnownColumns onCols, KnownColumns cols1, KnownColumns cols2
          , colsOut ~ JoinCols cols1 cols2 joinType, KnownColumns colsOut
          , All CanBeDFValue (MapTypes cols1), All CanBeDFValue (MapTypes cols2)
          )
       => DataFrame cols1 -> DataFrame cols2 -> JoinType -> DataFrame colsOut
joinDF df1 df2 joinType =
    let
        rows1 = toRows df1
        rows2 = toRows df2

        getJoinKey :: Row -> TypeLevelRow onCols
        getJoinKey = toTypeLevelRow @onCols

        map1 = Map.fromListWith (++) $ map (\r -> (getJoinKey r, [r])) rows1
        map2 = Map.fromListWith (++) $ map (\r -> (getJoinKey r, [r])) rows2

        -- Helper to convert DFValue to Maybe DFValue based on expected type
        toMaybeDFValue :: forall a. CanBeDFValue a => DFValue -> Maybe a
        toMaybeDFValue NA = Nothing
        toMaybeDFValue v = fromDFValue v

        -- Helper to convert Maybe DFValue to DFValue for DataFrame construction
        fromMaybeDFValue :: forall a. CanBeDFValue a => Maybe a -> DFValue
        fromMaybeDFValue Nothing = NA
        fromMaybeDFValue (Just x) = toDFValue x

        -- Get column names and types for the output DataFrame
        outputColNames = columnNames (Proxy @colsOut)

        -- A helper function to process a single column
        processColumn :: forall col. (KnownSymbol col) => Proxy col -> Row -> Row -> (T.Text, DFValue)
        processColumn colProxy r1 r2 =
            let colName = T.pack $ symbolVal colProxy
                val1 = fromMaybe NA (Map.lookup colName r1)
                val2 = fromMaybe NA (Map.lookup colName r2)
            in (colName, resolveJoinValueImpl val1 val2 joinType)

        -- Function to create a row for the output DataFrame, handling Maybe types
        createOutputRow :: Row -> Row -> Row
        createOutputRow r1 r2 = Map.fromList $ map (\colName ->
            case someSymbolVal (T.unpack colName) of
                SomeSymbol proxy -> processColumn proxy r1 r2
            ) outputColNames

        joinedRows = case joinType of
            InnerJoin ->
                Map.elems $ Map.intersectionWith (\rs1 rs2 -> [createOutputRow r1 r2 | r1 <- rs1, r2 <- rs2]) map1 map2
            LeftJoin ->
                Map.elems $ Map.mapWithKey (\k rs1 ->
                    let rs2 = fromMaybe [Map.fromList [(col, NA) | col <- columnNames (Proxy @cols2)]] (Map.lookup k map2)
                    in [createOutputRow r1 r2 | r1 <- rs1, r2 <- rs2]
                ) map1
            RightJoin ->
                Map.elems $ Map.mapWithKey (\k rs2 ->
                    let rs1 = fromMaybe [Map.fromList [(col, NA) | col <- columnNames (Proxy @cols1)]] (Map.lookup k map1)
                    in [createOutputRow r1 r2 | r1 <- rs1, r2 <- rs2]
                ) map2
            OuterJoin ->
                let allKeys = Map.keys $ Map.union map1 map2
                    blank1 = Map.fromList [(col, NA) | col <- columnNames (Proxy @cols1)]
                    blank2 = Map.fromList [(col, NA) | col <- columnNames (Proxy @cols2)]
                in concatMap (\k ->
                    let rs1 = fromMaybe [blank1] (Map.lookup k map1)
                        rs2 = fromMaybe [blank2] (Map.lookup k map2)
                    in [[createOutputRow r1 r2] | r1 <- rs1, r2 <- rs2]
                ) allKeys

    in fromRows @colsOut $ concat joinedRows
