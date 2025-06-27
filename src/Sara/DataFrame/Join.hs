module Sara.DataFrame.Join (
    joinDF
) where

import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.List (nub)
import Data.Maybe (fromMaybe) -- Added import
import Sara.DataFrame.Types (DataFrame(..), Column, Row, DFValue(..), JoinType(..), toRows)

-- | Joins two DataFrames based on common columns and a specified join type.
joinDF :: DataFrame -> DataFrame -> [T.Text] -> JoinType -> DataFrame
joinDF (DataFrame df1Map) (DataFrame df2Map) onCols joinType =
    let
        rows1 = toRows (DataFrame df1Map)
        rows2 = toRows (DataFrame df2Map)

        -- Helper to extract join key from a row
        getJoinKey :: Row -> Row
        getJoinKey row = Map.filterWithKey (\k _ -> k `elem` onCols) row

        -- Helper to combine two rows, handling overlapping columns
        combineRows :: Row -> Row -> Row
        combineRows r1 r2 = Map.union r1 r2 -- r2 overwrites r1 for common keys

        -- Perform the join based on JoinType
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
                    -- Collect all unique join keys
                    allKeys = nub $ map getJoinKey rows1 ++ map getJoinKey rows2

                    -- Create maps from join key to row for efficient lookup
                    map1 = Map.fromList (map (\r -> (getJoinKey r, r)) rows1)
                    map2 = Map.fromList (map (\r -> (getJoinKey r, r)) rows2)
                in
                    [ combineRows (fromMaybe (Map.map (\_ -> NA) (head rows1)) (Map.lookup k map1))
                                  (fromMaybe (Map.map (\_ -> NA) (head rows2)) (Map.lookup k map2))
                    | k <- allKeys
                    ]

        -- Convert joined rows back to DataFrame
        newDfMap = if null joinedRows
                   then Map.empty
                   else
                       let
                           colNames = Map.keys (head joinedRows)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- joinedRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap