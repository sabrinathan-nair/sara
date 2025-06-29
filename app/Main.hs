module Main where

import Sara.DataFrame.IO
import Sara.Visualization
import qualified Data.Text as T

main :: IO ()
main = do
    putStrLn "Reading people.csv..."
    df <- readCSV "people.csv"

    putStrLn "\nGenerating Vega-Lite JSON for a faceted scatter plot (Age vs. GPA, by IsStudent and City)..."
    let facetedPlotSpec = Plot
            { plotTitle = Just (T.pack "Age vs. GPA by Student Status and City")
            , plotDescription = Just (T.pack "A faceted scatter plot showing Age vs. GPA, separated by student status (rows) and city (columns).")
            , plotEncoding = PlotEncoding
                { encodingX = Just (X (T.pack "Age"))
                , encodingY = Just (Y (T.pack "GPA"))
                , encodingColor = Just (Color (T.pack "City"))
                , encodingSize = Nothing
                , encodingOpacity = Nothing
                , encodingShape = Nothing
                , encodingText = Nothing
                , encodingTooltip = Just (Tooltip (T.pack "Name"))
                , encodingGeom = Point
                }
            , plotData = df
            }

    putStrLn "Viewing faceted scatter plot in browser..."
    viewPlotInBrowser facetedPlotSpec

    putStrLn "Done with faceted plot."