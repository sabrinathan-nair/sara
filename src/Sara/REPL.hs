{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

-- | This module provides a placeholder for a Read-Eval-Print Loop (REPL)
-- for interacting with `DataFrame`s.
module Sara.REPL (
    repl
) where

import Sara.DataFrame.Types (DataFrame, KnownColumns)

-- | A placeholder REPL function.
-- This function is not yet implemented.
repl :: KnownColumns cols => DataFrame cols -> IO ()
repl _ = putStrLn "REPL functionality not yet implemented."