{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}

-- | This module provides functions for joining `DataFrame`s.
module Sara.DataFrame.Join (
    joinDF,
    CreateOutputRow
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

instance (KnownSymbol col, CanBeDFValue ty, CreateOutputRow rest) => CreateOutputRow ('(col, ty) ': rest) where
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
    let df1Rows = toRows df1
        df2Rows = toRows df2
        getJoinKey :: Row -> TypeLevelRow (SymbolsToSchema onCols cols1)
        getJoinKey = toTypeLevelRow @(SymbolsToSchema onCols cols1)
        df2Map = Map.fromListWith (++) $ map (\r -> (getJoinKey r, [r])) df2Rows

        joinedRows = concatMap (\r1 ->
            let key = getJoinKey r1
            in case Map.lookup key df2Map of
                Just matchingRows2 -> map (\r2 -> createOutputRow (Proxy @colsOut) r1 r2) matchingRows2
                Nothing -> []
            ) df1Rows
    in fromRows @colsOut joinedRows
