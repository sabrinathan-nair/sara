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
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleInstances #-}

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

-- | A helper typeclass for creating the output row of a join operation.
class CreateOutputRow (cols :: [(Symbol, Type)]) where
    createOutputRow :: Proxy cols -> Row -> Row -> Row

instance CreateOutputRow '[] where
    createOutputRow _ _ _ = Map.empty

instance (KnownSymbol col, CanBeDFValue (TypeOf col cols), CreateOutputRow rest) => CreateOutputRow ('(col, ty) ': rest) where
    createOutputRow _ r1 r2 =
        let colName = T.pack $ symbolVal (Proxy @col)
            val1 = Map.lookup colName r1
            val2 = Map.lookup colName r2
            val = fromMaybe NA (val1 <|> val2)
        in Map.insert colName val (createOutputRow (Proxy @rest) r1 r2)

joinDF :: forall (onCols :: [Symbol]) (cols1 :: [(Symbol, Type)]) (cols2 :: [(Symbol, Type)]) (colsOut :: [(Symbol, Type)])
       . ( HasColumns onCols cols1, HasColumns onCols cols2
         , KnownSymbols onCols, KnownColumns (SymbolsToSchema onCols cols1)
         , KnownColumns cols1, KnownColumns cols2
         , colsOut ~ JoinCols cols1 cols2, KnownColumns colsOut
         , All CanBeDFValue (GetColumnTypes cols1)
         , All CanBeDFValue (GetColumnTypes cols2)
         , All CanBeDFValue (GetColumnTypes colsOut)
         , CreateOutputRow colsOut
         ) => DataFrame cols1 -> DataFrame cols2 -> DataFrame colsOut
joinDF df1 df2 = 
    let 
        rows1 = toRows df1
        rows2 = toRows df2

        getJoinKey :: Row -> TypeLevelRow (SymbolsToSchema onCols cols1)
        getJoinKey = toTypeLevelRow @(SymbolsToSchema onCols cols1)

        map1 = Map.fromListWith (++) $ map (\r -> (getJoinKey r, [r])) rows1
        map2 = Map.fromListWith (++) $ map (\r -> (getJoinKey r, [r])) rows2

        processRow :: [Row] -> [Row]
        processRow rs1 = 
            let 
                key = getJoinKey (head rs1)
            in 
                case Map.lookup key map2 of
                    Just rs2 -> [createOutputRow (Proxy @colsOut) r1 r2 | r1 <- rs1, r2 <- rs2]
                    Nothing -> []
        
        joinedRows = concatMap processRow (Map.elems map1)
    in 
        fromRows @colsOut joinedRows

