{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DataKinds #-}

module MutateShouldFail where

import Sara.DataFrame.Transform (mutate)
import Sara.DataFrame.Expression (col, lit)
import Sara.DataFrame.Types
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

main :: IO ()
main = do
    let df = fromRows @'[ '("Name", T.Text), '("Age", Int)] [
            Map.fromList [("Name", TextValue "Alice"), ("Age", IntValue 30)]
            ]
    let expr = col (Proxy @"Name") +.+ lit (5 :: Int)
    let _ = mutate @"InvalidCol" (Proxy @"InvalidCol") expr df
    return ()