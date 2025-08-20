module Main (main) where

import Test.QuickCheck
import qualified Data.Vector as V
import qualified Data.ByteString.Char8 as BC
import Sara.DataFrame.Static (collectColumnSamplesText)

prop_NoCrashAndRespectsHeader :: [BC.ByteString] -> [[BC.ByteString]] -> Bool
prop_NoCrashAndRespectsHeader hdr rawRows =
  let header = V.fromList hdr
      rows   = map V.fromList rawRows
      cols   = collectColumnSamplesText header rows
  in length cols == V.length header

main :: IO ()
main = quickCheck prop_NoCrashAndRespectsHeader
