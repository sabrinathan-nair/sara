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
import Streaming (Stream, Of)
import qualified Streaming.Prelude as S
import Control.Monad.IO.Class (liftIO)

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
         ) => Stream (Of (DataFrame cols1)) IO () -> Stream (Of (DataFrame cols2)) IO () -> Stream (Of (DataFrame colsOut)) IO ()
joinDF df1Stream df2Stream = do
    -- Read the second stream into memory for efficient lookups
    df2sList <- liftIO $ S.toList_ df2Stream
    let df2Rows = concatMap toRows df2sList
    let getJoinKey :: Row -> TypeLevelRow (SymbolsToSchema onCols cols1)
        getJoinKey = toTypeLevelRow @(SymbolsToSchema onCols cols1)
    let df2Map = Map.fromListWith (++) $ map (\r -> (getJoinKey r, [r])) df2Rows

    -- Process the first stream
    S.for df1Stream $ \df1 -> do
        let df1Rows = toRows df1
        S.for (S.each df1Rows) $ \r1 -> do
            let key = getJoinKey r1
            case Map.lookup key df2Map of
                Just matchingRows2 -> do
                    S.for (S.each matchingRows2) $ \r2 -> do
                        let joinedRow = createOutputRow (Proxy @colsOut) r1 r2
                        S.yield (fromRows @colsOut [joinedRow])
                Nothing -> return ()