{-# LANGUAGE ViewPatterns #-}
module Main where

import Test.Hspec
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Sara.DataFrame.IO
import Sara.DataFrame.Transform
import Sara.DataFrame.Types
import Sara.DataFrame.TimeSeries
import Sara.DataFrame.Missing (fillna, ffill, bfill, dropna, DropAxis(..))
import Data.Time.Format (parseTimeM, defaultTimeLocale)
import Data.Time (UTCTime, Day)

main :: IO ()
main = hspec $ do
  let parseTimeOrError :: String -> String -> UTCTime
      parseTimeOrError formatStr timeStr = case parseTimeM True defaultTimeLocale formatStr timeStr of
        Just t -> t
        Nothing -> error $ "Failed to parse time: " ++ timeStr

  -- Helper for approximate double comparison
  let shouldBeApprox :: Double -> Double -> Expectation
      shouldBeApprox actual expected = actual `shouldSatisfy` (\x -> abs (x - expected) < 1e-9)

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

  let createTimeSeriesDataFrame :: IO DataFrame
      createTimeSeriesDataFrame = do
        let rows = [
                Map.fromList [(T.pack "Date", TimestampValue (parseTimeOrError "%Y-%m-%d %H:%M:%S" "2023-01-01 10:00:00")), (T.pack "Value", IntValue 10)],
                Map.fromList [(T.pack "Date", TimestampValue (parseTimeOrError "%Y-%m-%d %H:%M:%S" "2023-01-01 11:00:00")), (T.pack "Value", IntValue 20)],
                Map.fromList [(T.pack "Date", TimestampValue (parseTimeOrError "%Y-%m-%d %H:%M:%S" "2023-01-02 10:00:00")), (T.pack "Value", IntValue 15)],
                Map.fromList [(T.pack "Date", TimestampValue (parseTimeOrError "%Y-%m-%d %H:%M:%S" "2023-01-02 11:00:00")), (T.pack "Value", IntValue 25)],
                Map.fromList [(T.pack "Date", TimestampValue (parseTimeOrError "%Y-%m-%d %H:%M:%S" "2023-01-03 10:00:00")), (T.pack "Value", IntValue 30)]
                ]
        return $ fromRows rows

  let parseTimeOrError :: String -> String -> UTCTime
      parseTimeOrError formatStr timeStr = case parseTimeM True defaultTimeLocale formatStr timeStr of
        Just t -> t
        Nothing -> error $ "Failed to parse time: " ++ timeStr

  describe "TimeSeries" $ do
    it "resamples daily data to monthly (sum)" $ do
      df <- createTimeSeriesDataFrame
      let resampledDf = resample df "Date" Monthly (\vec -> DoubleValue $ V.sum $ V.map (\val -> case val of IntValue i -> fromIntegral i; DoubleValue d -> d; _ -> 0.0) vec)
      let (DataFrame resampledMap) = resampledDf
      Map.keys resampledMap `shouldMatchList` [T.pack "Date", T.pack "Value"]
      case Map.lookup (T.pack "Value") resampledMap of
        Just (V.toList -> [DoubleValue val]) -> val `shouldBe` 100.0
        _ -> expectationFailure "Resampled Value column not found or incorrect"

    it "calculates rolling average" $ do
      df <- createTimeSeriesDataFrame
      let rollingDf = rolling df 2 "Value"
      let (DataFrame rollingMap) = rollingDf
      Map.keys rollingMap `shouldMatchList` [T.pack "Date", T.pack "Value", T.pack "Value_rolling"]
      case Map.lookup (T.pack "Value_rolling") rollingMap of
        Just (V.toList -> [DoubleValue r1, DoubleValue r2, DoubleValue r3, DoubleValue r4, DoubleValue r5]) -> do
          r1 `shouldBe` 15.0
          r2 `shouldBe` 17.5
          r3 `shouldBe` 20.0
          r4 `shouldBe` 27.5
          r5 `shouldBe` 30.0
        _ -> expectationFailure "Rolling Value column not found or incorrect"

    it "shifts column values" $ do
      df <- createTimeSeriesDataFrame
      let shiftedDf = shift df "Value" 1
      let (DataFrame shiftedMap) = shiftedDf
      Map.keys shiftedMap `shouldMatchList` [T.pack "Date", T.pack "Value", T.pack "Value_shifted"]
      case Map.lookup (T.pack "Value_shifted") shiftedMap of
        Just (V.toList -> [NA, IntValue s1, IntValue s2, IntValue s3, IntValue s4]) -> do
          s1 `shouldBe` 10
          s2 `shouldBe` 20
          s3 `shouldBe` 15
          s4 `shouldBe` 25
        _ -> expectationFailure "Shifted Value column not found or incorrect"

    it "calculates percentage change" $ do
      df <- createTimeSeriesDataFrame
      let pctChangeDf = pctChange df "Value"
      let (DataFrame pctChangeMap) = pctChangeDf
      Map.keys pctChangeMap `shouldMatchList` [T.pack "Date", T.pack "Value", T.pack "Value_pct_change"]
      case Map.lookup (T.pack "Value_pct_change") pctChangeMap of
        Just col -> do
          print col -- Debug print
          case V.toList col of
            [NA, DoubleValue p1, DoubleValue p2, DoubleValue p3, DoubleValue p4] -> do
              p1 `shouldBeApprox` 1.0
              p2 `shouldBeApprox` (-0.25)
              p3 `shouldBeApprox` (2/3)
              p4 `shouldBeApprox` 0.2
            _ -> expectationFailure $ "Percentage Change column content incorrect: " ++ show col
        _ -> expectationFailure "Percentage Change column not found or incorrect"

  describe "Missing Data Handling" $ do
    let createNaDataFrame :: IO DataFrame
        createNaDataFrame = do
          let rows = [
                  Map.fromList [(T.pack "Col1", IntValue 1), (T.pack "Col2", NA), (T.pack "Col3", DoubleValue 1.1)],
                  Map.fromList [(T.pack "Col1", NA), (T.pack "Col2", IntValue 2), (T.pack "Col3", NA)],
                  Map.fromList [(T.pack "Col1", IntValue 3), (T.pack "Col2", NA), (T.pack "Col3", DoubleValue 3.3)]
                  ]
          return $ fromRows rows

    it "fills NA values with a specified value" $ do
      df <- createNaDataFrame
      let filledDf = fillna df Nothing (IntValue 0)
      let (DataFrame filledMap) = filledDf
      case Map.lookup (T.pack "Col2") filledMap of
        Just (V.toList -> [IntValue v1, IntValue v2, IntValue v3]) -> do
          v1 `shouldBe` 0
          v2 `shouldBe` 2
          v3 `shouldBe` 0
        _ -> expectationFailure "fillna failed for Col2"

    it "forward fills NA values" $ do
      df <- createNaDataFrame
      let ffilledDf = ffill df
      let (DataFrame ffilledMap) = ffilledDf
      case Map.lookup (T.pack "Col2") ffilledMap of
        Just col -> do
          -- print col -- Debug print
          case V.toList col of
            [NA, IntValue v2, IntValue v3] -> do
              v2 `shouldBe` 2
              v3 `shouldBe` 2
            _ -> expectationFailure $ "ffill failed for Col2. Actual: " ++ show col

    it "backward fills NA values" $ do
      df <- createNaDataFrame
      let bfilledDf = bfill df
      let (DataFrame bfilledMap) = bfilledDf
      case Map.lookup (T.pack "Col2") bfilledMap of
        Just col -> do
          -- print col -- Debug print
          case V.toList col of
            [IntValue v1, IntValue v2, NA] -> do
              v1 `shouldBe` 2
              v2 `shouldBe` 2
            _ -> expectationFailure $ "bfill failed for Col2. Actual: " ++ show col

    it "drops rows with any NA values" $ do
      df <- createNaDataFrame
      let droppedDf = dropna df DropRows Nothing
      let (DataFrame droppedMap) = droppedDf
      toRows droppedDf `shouldBe` []

    it "drops rows with a threshold of NA values" $ do
      df <- createNaDataFrame
      let droppedDf = dropna df DropRows (Just 2)
      let (DataFrame droppedMap) = droppedDf
      let expectedRows = [
              Map.fromList [(T.pack "Col1", IntValue 1), (T.pack "Col2", NA), (T.pack "Col3", DoubleValue 1.1)],
              Map.fromList [(T.pack "Col1", IntValue 3), (T.pack "Col2", NA), (T.pack "Col3", DoubleValue 3.3)]
              ]
      toRows droppedDf `shouldBe` expectedRows

    it "drops columns with any NA values" $ do
      df <- createNaDataFrame
      let droppedDf = dropna df DropColumns Nothing
      let (DataFrame droppedMap) = droppedDf
      Map.keys droppedMap `shouldMatchList` []

    it "drops columns with a threshold of NA values" $ do
      df <- createNaDataFrame
      let droppedDf = dropna df DropColumns (Just 2)
      let (DataFrame droppedMap) = droppedDf
      Map.keys droppedMap `shouldMatchList` [T.pack "Col1", T.pack "Col3"]
