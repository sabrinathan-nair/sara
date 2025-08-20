{-# OPTIONS_GHC -Wno-orphans #-}
module Main (main) where

import Test.Hspec
import Test.QuickCheck
import qualified Data.Vector as V
import qualified Data.ByteString.Char8 as BC
import qualified Data.Text as T

import Sara.DataFrame.Static (collectColumnSamplesText)

-- Generators for ragged CSV-like rows
genCell :: Gen BC.ByteString
genCell = BC.pack <$> listOf (elements (['a'..'z'] ++ ['0'..'9'] ++ " -_."))

genRow :: Int -> Gen (V.Vector BC.ByteString)
genRow maxLen = do
  -- ragged: choose arbitrary length between 0 and maxLen
  n <- chooseInt (0, maxLen)
  V.fromList <$> vectorOf n genCell

main :: IO ()
main = hspec $ do
  describe "collectColumnSamplesText" $ do
    it "never throws on ragged rows and respects header length" $
      property $ \hdrN -> forAll (chooseInt (0, 8)) $ \nHdrCols ->
        forAll (vectorOf nHdrCols genCell) $ \hdrCells ->
          forAll (chooseInt (0, 50)) $ \nRows ->
            forAll (vectorOf nRows (genRow 12)) $ \rows ->
              let header = V.fromList hdrCells
                  cols   = collectColumnSamplesText header rows
              in length cols == V.length header

    it "pads short rows with empty cells" $ do
      let header = V.fromList [BC.pack "A", BC.pack "B", BC.pack "C"]
          rows   =
            [ V.fromList [BC.pack "1"]                         -- short
            , V.fromList [BC.pack "2", BC.pack "x", BC.pack "y"] -- exact
            , V.fromList []                                    -- empty
            ]
          cols = collectColumnSamplesText header rows
          colA = map T.unpack (cols !! 0)
          colB = map T.unpack (cols !! 1)
          colC = map T.unpack (cols !! 2)
      colA `shouldBe` ["1","2",""]
      colB `shouldBe` ["","x",""]
      colC `shouldBe` ["","y",""]

    it "decodes bytes to Text UTF-8 (no crashes on invalid bytes; treated as Latin-1 by pack)" $ do
      let header = V.fromList [BC.pack "α", BC.pack "β"]
          rows   = [ V.fromList [BC.pack "π", BC.pack "σ"] ]
          cols   = collectColumnSamplesText header rows
      length cols `shouldBe` 2
      map length cols `shouldBe` [1,1]
