{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Sara.REPL (
    repl
) where

import Sara.DataFrame.Types (DataFrame, KnownColumns)

-- | A placeholder REPL function.
repl :: KnownColumns cols => DataFrame cols -> IO ()
repl _ = putStrLn "REPL functionality not yet implemented."