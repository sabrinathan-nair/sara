{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

module Sara.DataFrame.Join (
    joinDF
) where



import qualified Data.Map.Strict as Map


import Data.Maybe (fromMaybe)
import Sara.DataFrame.Types (DFValue(..), Row, DataFrame(..), JoinType(..), toRows, fromRows, KnownColumns, HasColumns, JoinCols, columnNames, MapSymbols, TypeLevelRow, toTypeLevelRow)
import GHC.TypeLits
import Data.Proxy (Proxy(..))
import Data.Kind (Type)

-- | Joins two DataFrames based on common columns and a specified join type.
joinDF :: forall (onCols :: [(Symbol, Type)]) (cols1 :: [(Symbol, Type)]) (cols2 :: [(Symbol, Type)]) (colsOut :: [(Symbol, Type)]).
          ( HasColumns (MapSymbols onCols) cols1, HasColumns (MapSymbols onCols) cols2
          , KnownColumns onCols, KnownColumns cols1, KnownColumns cols2
          , colsOut ~ JoinCols cols1 cols2, KnownColumns colsOut
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

        combineRows :: Row -> Row -> Row
        combineRows = Map.union

        joinedRows = case joinType of
            InnerJoin ->
                Map.elems $ Map.intersectionWith (\rs1 rs2 -> [combineRows r1 r2 | r1 <- rs1, r2 <- rs2]) map1 map2
            LeftJoin ->
                Map.elems $ Map.mapWithKey (\k rs1 ->
                    let rs2 = fromMaybe [Map.fromList [(col, NA) | col <- columnNames (Proxy @cols2)]] (Map.lookup k map2)
                    in [combineRows r1 r2 | r1 <- rs1, r2 <- rs2]
                ) map1
            RightJoin ->
                Map.elems $ Map.mapWithKey (\k rs2 ->
                    let rs1 = fromMaybe [Map.fromList [(col, NA) | col <- columnNames (Proxy @cols1)]] (Map.lookup k map2)
                    in [combineRows r1 r2 | r1 <- rs1, r2 <- rs2]
                ) map2
            OuterJoin ->
                let allKeys = Map.keys $ Map.union map1 map2
                    blank1 = Map.fromList [(col, NA) | col <- columnNames (Proxy @cols1)]
                    blank2 = Map.fromList [(col, NA) | col <- columnNames (Proxy @cols2)]
                in concatMap (\k ->
                    let rs1 = fromMaybe [blank1] (Map.lookup k map1)
                        rs2 = fromMaybe [blank2] (Map.lookup k map2)
                    in [[combineRows r1 r2 | r1 <- rs1, r2 <- rs2]]
                ) allKeys

    in fromRows @colsOut $ concat joinedRows