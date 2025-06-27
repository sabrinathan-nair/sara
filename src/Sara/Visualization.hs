module Sara.Visualization (
    Aesthetic(..),
    Geom(..),
    PlotEncoding(..),
    Plot(..),
    toVegaLite,
    savePlotAsVegaLite,
    viewPlotInBrowser
) where

import qualified Data.Text as T
import Sara.DataFrame.Types (DFValue(..), DataFrame(..), toRows) -- Import DataFrame and toRows
import Data.Aeson as A
import Data.Aeson.Text (encodeToLazyText)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TLIO
import qualified Data.Vector as V
import Data.Maybe (catMaybes)
import qualified Data.Aeson.Key as Aeson.Key
import qualified Data.Map.Strict as Map
import Data.Time.Format (formatTime, defaultTimeLocale)
import qualified Sara.DataFrame.Types as Types -- Qualified import for DFValue
import System.Process (callCommand)
import System.FilePath ((</>))
import System.Directory (getCurrentDirectory)

-- | Defines an aesthetic mapping for a plot.
-- Maps a column name (Text) to a visual property.
data Aesthetic = X T.Text
               | Y T.Text
               | Color T.Text
               | Size T.Text
               | Opacity T.Text
               | Shape T.Text
               | Text T.Text
               | Tooltip T.Text
               -- Add more as needed
               deriving (Show, Eq)

instance ToJSON Aesthetic where
    toJSON (X col) = A.object [Aeson.Key.fromText (T.pack "field") .= col, Aeson.Key.fromText (T.pack "type") .= T.pack "quantitative"]
    toJSON (Y col) = A.object [Aeson.Key.fromText (T.pack "field") .= col, Aeson.Key.fromText (T.pack "type") .= T.pack "quantitative"]
    toJSON (Color col) = A.object [Aeson.Key.fromText (T.pack "field") .= col, Aeson.Key.fromText (T.pack "type") .= T.pack "nominal"]
    toJSON (Size col) = A.object [Aeson.Key.fromText (T.pack "field") .= col, Aeson.Key.fromText (T.pack "type") .= T.pack "quantitative"]
    toJSON (Opacity col) = A.object [Aeson.Key.fromText (T.pack "field") .= col, Aeson.Key.fromText (T.pack "type") .= T.pack "quantitative"]
    toJSON (Shape col) = A.object [Aeson.Key.fromText (T.pack "field") .= col, Aeson.Key.fromText (T.pack "type") .= T.pack "nominal"]
    toJSON (Text col) = A.object [Aeson.Key.fromText (T.pack "field") .= col, Aeson.Key.fromText (T.pack "type") .= T.pack "nominal"]
    toJSON (Tooltip col) = A.object [Aeson.Key.fromText (T.pack "field") .= col, Aeson.Key.fromText (T.pack "type") .= T.pack "nominal"]

-- | Defines the type of geometric mark to use in the plot.
data Geom = Point
          | Bar
          | Line
          | Area
          | TextGeom
          -- Add more as needed
          deriving (Show, Eq)

-- Helper to convert Geom to Aeson Value
geomToAeson :: Geom -> A.Value
geomToAeson Point = A.String (T.pack "point")
geomToAeson Bar = A.String (T.pack "bar")
geomToAeson Line = A.String (T.pack "line")
geomToAeson Area = A.String (T.pack "area")
geomToAeson TextGeom = A.String (T.pack "text")

-- | Combines aesthetic mappings and a geometric mark.
data PlotEncoding = PlotEncoding -- Renamed from Encoding
    { encodingX :: Maybe Aesthetic
    , encodingY :: Maybe Aesthetic
    , encodingColor :: Maybe Aesthetic
    , encodingSize :: Maybe Aesthetic
    , encodingOpacity :: Maybe Aesthetic
    , encodingShape :: Maybe Aesthetic
    , encodingText :: Maybe Aesthetic
    , encodingTooltip :: Maybe Aesthetic
    , encodingGeom :: Geom
    } deriving (Show, Eq)

-- | Represents a complete plot specification.
data Plot = Plot
    { plotTitle :: Maybe T.Text
    , plotDescription :: Maybe T.Text
    , plotEncoding :: PlotEncoding -- Updated type
    , plotData :: DataFrame -- Added DataFrame to Plot
    -- Add more plot-level properties as needed (e.g., data source, width, height)
    } deriving (Show, Eq)

-- | Converts a DFValue to an Aeson Value for JSON serialization.
valueToAeson :: Types.DFValue -> A.Value -- Qualified DFValue
valueToAeson (Types.IntValue i) = A.toJSON i
valueToAeson (Types.DoubleValue d) = A.toJSON d
valueToAeson (Types.TextValue t) = A.toJSON t
valueToAeson (Types.DateValue d) = A.toJSON (formatTime defaultTimeLocale "%Y-%m-%d" d)
valueToAeson (Types.BoolValue b) = A.toJSON b
valueToAeson Types.NA = A.Null

-- | Converts a DataFrame into a list of Aeson Objects (rows) for Vega-Lite.
dataFrameToVegaLiteValues :: DataFrame -> A.Value
dataFrameToVegaLiteValues df =
    let
        rows = toRows df
        -- Convert each Row (Map Text DFValue) to an Aeson Object (Map Text A.Value)
        jsonRows = map (A.object . Map.toList . Map.mapKeys Aeson.Key.fromText . Map.map valueToAeson) rows
    in
        A.Array (V.fromList jsonRows)

-- | Converts a Plot to its Vega-Lite JSON representation.
toVegaLite :: Plot -> TL.Text
toVegaLite (Plot title desc encoding df) =
    let
        encObj = A.object $ catMaybes
            [ fmap ((Aeson.Key.fromText (T.pack "x")) .=) (encodingX encoding)
            , fmap ((Aeson.Key.fromText (T.pack "y")) .=) (encodingY encoding)
            , fmap ((Aeson.Key.fromText (T.pack "color")) .=) (encodingColor encoding)
            , fmap ((Aeson.Key.fromText (T.pack "size")) .=) (encodingSize encoding)
            , fmap ((Aeson.Key.fromText (T.pack "opacity")) .=) (encodingOpacity encoding)
            , fmap ((Aeson.Key.fromText (T.pack "shape")) .=) (encodingShape encoding)
            , fmap ((Aeson.Key.fromText (T.pack "text")) .=) (encodingText encoding)
            , fmap ((Aeson.Key.fromText (T.pack "tooltip")) .=) (encodingTooltip encoding)
            ]

        markObj = geomToAeson (encodingGeom encoding)

        -- Basic Vega-Lite spec structure
        vlSpec :: A.Value
        vlSpec = A.object $ catMaybes
            [ Just ((Aeson.Key.fromText (T.pack "$schema")) .= T.pack "https://vega.github.io/schema/vega-lite/v5.json")
            , fmap ((Aeson.Key.fromText (T.pack "title")) .=) title
            , fmap ((Aeson.Key.fromText (T.pack "description")) .=) desc
            , Just ((Aeson.Key.fromText (T.pack "mark")) .= markObj)
            , Just ((Aeson.Key.fromText (T.pack "encoding")) .= encObj)
            , Just ((Aeson.Key.fromText (T.pack "data")) .= dataFrameToVegaLiteValues df)
            ]
    in
        encodeToLazyText vlSpec

-- | Saves a Plot as a Vega-Lite JSON file.
savePlotAsVegaLite :: FilePath -> Plot -> IO ()
savePlotAsVegaLite filePath plot = do
    let jsonContent = toVegaLite plot
    TLIO.writeFile filePath jsonContent

-- | Generates an HTML file with the Vega-Lite plot and opens it in a browser.
viewPlotInBrowser :: Plot -> IO ()
viewPlotInBrowser plot = do
    currentDir <- getCurrentDirectory
    let htmlFilePath = currentDir </> "vega_lite_plot.html"
        jsonContent = toVegaLite plot
        htmlContent = TL.unlines
            [TL.pack "<!DOCTYPE html>"
            ,TL.pack "<html>"
            ,TL.pack "<head>"
            ,TL.pack "    <title>Vega-Lite Plot</title>"
            ,TL.pack "    <script src=\"https://cdn.jsdelivr.net/npm/vega@5\"></script>"
            ,TL.pack "    <script src=\"https://cdn.jsdelivr.net/npm/vega-lite@5\"></script>"
            ,TL.pack "    <script src=\"https://cdn.jsdelivr.net/npm/vega-embed@6\"></script>"
            ,TL.pack "</head>"
            ,TL.pack "<body>"
            ,TL.pack "    <div id=\"vis\"></div>"
            ,TL.pack "    <script type=\"text/javascript\">"
            ,TL.pack "        var spec = " <> jsonContent <> TL.pack ";"
            ,TL.pack "        vegaEmbed('#vis', spec).then(result => console.log(result)).catch(console.error);"
            ,TL.pack "    </script>"
            ,TL.pack "</body>"
            ,TL.pack "</html>"
            ]
    TLIO.writeFile htmlFilePath htmlContent
    putStrLn $ "Opening " ++ htmlFilePath ++ " in browser..."
    callCommand $ "xdg-open " ++ htmlFilePath -- For Linux
    -- For macOS: callCommand $ "open " ++ htmlFilePath
    -- For Windows: callCommand $ "start " ++ htmlFilePath