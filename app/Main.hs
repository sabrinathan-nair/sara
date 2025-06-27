module Main where

import Sara.DataFrame.IO
import Sara.DataFrame.Types
import Sara.DataFrame.Transform
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.Time (fromGregorian)

main :: IO ()
main = do
    putStrLn "Reading people.csv..."
    df <- readCSV "people.csv"
    putStrLn "Original DataFrame:"
    print df

    putStrLn "\nMelting DataFrame (id_vars: Name, City; value_vars: Age, GPA)..."
    let meltedDf = melt [T.pack "Name", T.pack "City"] [T.pack "Age", T.pack "GPA"] df
    putStrLn "Melted DataFrame:"
    print meltedDf