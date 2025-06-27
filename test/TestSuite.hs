{-# LANGUAGE ViewPatterns #-}
module Main where

import Test.Hspec
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.IO
import Sara.DataFrame.Transform
import Sara.DataFrame.Types

main :: IO ()
main = hspec $ do
  describe "readCSV" $ do
    it "reads a CSV file into a DataFrame with correct headers and data" $ do
      df <- readCSV "people.csv"
      let expectedNames = [T.pack "Name", T.pack "Age", T.pack "City", T.pack "IsStudent", T.pack "EnrollmentDate", T.pack "GPA"]
      let (DataFrame dfMap) = df
      Map.keys dfMap `shouldMatchList` expectedNames

      -- Test some specific values
      case Map.lookup (T.pack "Name") dfMap of
        Just (V.toList -> [TextValue name1, TextValue name2, _, _, _]) -> do
          name1 `shouldBe` T.pack "Alice"
          name2 `shouldBe` T.pack "Bob"
        _ -> expectationFailure "Name column not found or incorrect"

      case Map.lookup (T.pack "Age") dfMap of
        Just (V.toList -> [IntValue age1, IntValue age2, _, _, _]) -> do
          age1 `shouldBe` 30
          age2 `shouldBe` 24
        _ -> expectationFailure "Age column not found or incorrect"

  describe "selectColumns" $ do
    it "selects specified columns from a DataFrame" $ do
      df <- readCSV "people.csv"
      let selectedDf = selectColumns [T.pack "Name", T.pack "Age"] df
      let (DataFrame selectedMap) = selectedDf
      Map.keys selectedMap `shouldMatchList` [T.pack "Name", T.pack "Age"]

      -- Ensure data in selected columns is correct
      case Map.lookup (T.pack "Name") selectedMap of
        Just (V.toList -> [TextValue name1, TextValue name2, _, _, _]) -> do
          name1 `shouldBe` T.pack "Alice"
          name2 `shouldBe` T.pack "Bob"
        _ -> expectationFailure "Name column not found or incorrect in selected DataFrame"

      case Map.lookup (T.pack "Age") selectedMap of
        Just (V.toList -> [IntValue age1, IntValue age2, _, _, _]) -> do
          age1 `shouldBe` 30
          age2 `shouldBe` 24
        _ -> expectationFailure "Age column not found or incorrect in selected DataFrame"

    it "handles non-existent columns gracefully" $ do
      df <- readCSV "people.csv"
      let selectedDf = selectColumns [T.pack "Name", T.pack "NonExistent"] df
      let (DataFrame selectedMap) = selectedDf
      Map.keys selectedMap `shouldMatchList` [T.pack "Name"]
