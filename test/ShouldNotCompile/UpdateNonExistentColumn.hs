{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}

module ShouldNotCompile.UpdateNonExistentColumn where

import Sara.DataFrame.Types
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

-- This should fail to compile because 'NonExistentColumn' does not exist
-- in the schema.

exampleDataFrame :: DataFrame '[ '("ColA", Int), '("ColB", Double)]
exampleDataFrame = DataFrame Map.empty

updatedDataFrame :: DataFrame (UpdateColumn "NonExistentColumn" Bool '[ '("ColA", Int), '("ColB", Double)])
updatedDataFrame = undefined
