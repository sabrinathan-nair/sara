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

import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.List (nub)
import Data.Maybe (fromMaybe)
import Sara.DataFrame.Types (DFValue(..), Column, Row, DataFrame(..), JoinType(..), toRows, KnownColumns, HasColumns, JoinCols, columnNames, MapSymbols)
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
        onColsNames = columnNames (Proxy @onCols)

        getJoinKey :: Row -> Row
        getJoinKey row = Map.filterWithKey (\k _ -> k `elem` onColsNames) row

        combineRows :: Row -> Row -> Row
        combineRows r1 r2 = Map.union r1 r2

        joinedRows = case joinType of
            InnerJoin ->
                [ combineRows r1 r2
                | r1 <- rows1
                , r2 <- rows2
                , getJoinKey r1 == getJoinKey r2
                ]
            LeftJoin ->
                [ combineRows r1 (fromMaybe Map.empty (Map.lookup (getJoinKey r1) (Map.fromList (map (\r -> (getJoinKey r, r)) rows2))))
                | r1 <- rows1
                ]
            RightJoin ->
                [ combineRows (fromMaybe Map.empty (Map.lookup (getJoinKey r2) (Map.fromList (map (\r -> (getJoinKey r, r)) rows1)))) r2
                | r2 <- rows2
                ]
            OuterJoin ->
                let
                    allKeys = nub $ map getJoinKey rows1 ++ map getJoinKey rows2
                    map1 = Map.fromList (map (\r -> (getJoinKey r, r)) rows1)
                    map2 = Map.fromList (map (\r -> (getJoinKey r, r)) rows2)
                in
                    [ combineRows (fromMaybe (Map.map (\_ -> NA) (head rows1)) (Map.lookup k map1))
                                  (fromMaybe (Map.map (\_ -> NA) (head rows2)) (Map.lookup k map2))
                    | k <- allKeys
                    ]

        newDfMap = if null joinedRows
                   then Map.empty
                   else
                       let
                           colNames = columnNames (Proxy @colsOut)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- joinedRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap