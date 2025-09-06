{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Sara.DataFrame.Stream
  ( DFStream
  , RowStream
  , toRowStream
  , fromRowStream
  ) where

import Streaming (Stream, Of)
import qualified Streaming.Prelude as S

import qualified Data.Map.Strict as Map
import qualified Data.Text as T

import Sara.DataFrame.Types
  ( DFValue(..)
  , DataFrame(..)
  , KnownColumns
  , toRows
  , fromRows
  )

-- | Row representation (a map of column name to DFValue).
type Row = Map.Map T.Text DFValue

-- | Stream of DataFrame chunks (typed by schema).
type DFStream cols = Stream (Of (DataFrame cols)) IO ()

-- | Row-by-row stream (not parameterized by cols).
type RowStream = Stream (Of Row) IO ()

-- | Flatten a stream of DataFrame chunks into a row stream.
toRowStream :: KnownColumns cols => DFStream cols -> RowStream
toRowStream = S.concat . S.map toRows

-- | Convert a row stream into a stream of DataFrame chunks.
-- Emits one-row DataFrames. (Can be rebatched later if needed.)
fromRowStream :: KnownColumns cols => RowStream -> DFStream cols
fromRowStream = S.map (fromRows . (:[]))
