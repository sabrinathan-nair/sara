module Sara.DataFrame.Wrangling (
    filterRows,
    sortDataFrame,
    dropColumns,
    dropRows,
    renameColumns,
    dropNA,
    fillNA
) where

import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import Data.List (sortBy)
import Data.Ord (comparing)
import Sara.DataFrame.Types (DataFrame(..), Column, Row, toRows, DFValue(..), SortOrder(..))

-- | Filters rows from a DataFrame based on a predicate function.
filterRows :: (Row -> Bool) -> DataFrame -> DataFrame
filterRows p df =
    let
        rows = toRows df
        filteredRows = filter p rows
        -- Convert filtered rows back to DataFrame
        -- This assumes all rows have the same columns, which should be true for a DataFrame
        newDfMap = if null filteredRows
                   then Map.empty
                   else
                       let
                           -- Get column names from the first row
                           colNames = Map.keys (head filteredRows)
                           -- Create a list of columns, where each column is a Vector of DFValues
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- filteredRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap

-- | Sorts a DataFrame based on a list of column names and their sort orders.
sortDataFrame :: [(T.Text, SortOrder)] -> DataFrame -> DataFrame
sortDataFrame sortCriteria df =
    let
        rows = toRows df

        -- Custom comparison function for rows
        compareRows :: Row -> Row -> Ordering
        compareRows r1 r2 = foldr (\(colName, sortOrder) acc ->
                let
                    val1 = Map.findWithDefault NA colName r1
                    val2 = Map.findWithDefault NA colName r2
                in
                    case compare val1 val2 of
                        EQ -> acc
                        res -> case sortOrder of
                                 Ascending -> res
                                 Descending -> invertOrdering res
            ) EQ sortCriteria

        invertOrdering :: Ordering -> Ordering
        invertOrdering LT = GT
        invertOrdering EQ = EQ
        invertOrdering GT = LT

        sortedRows = sortBy compareRows rows

        -- Convert sorted rows back to DataFrame
        newDfMap = if null sortedRows
                   then Map.empty
                   else
                       let
                           colNames = Map.keys (head sortedRows)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- sortedRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap

-- | Drops a list of columns from a DataFrame.
dropColumns :: [T.Text] -> DataFrame -> DataFrame
dropColumns colsToDrop (DataFrame dfMap) =
    let
        newDfMap = foldr Map.delete dfMap colsToDrop
    in
        DataFrame newDfMap

-- | Drops rows from a DataFrame based on a list of row indices.
dropRows :: [Int] -> DataFrame -> DataFrame
dropRows indicesToDrop (DataFrame dfMap) =
    let
        -- Get the number of rows from any column (assuming all columns have same length)
        numRows = if Map.null dfMap then 0 else V.length (snd . head . Map.toList $ dfMap)
        
        -- Create a new vector of indices to keep
        indicesToKeep = V.fromList [ i | i <- [0 .. numRows - 1], i `notElem` indicesToDrop ]

        -- Create a new DataFrame by filtering each column
        newDfMap = Map.map (\col -> V.map (col V.!) indicesToKeep) dfMap
    in
        DataFrame newDfMap

-- | Renames columns in a DataFrame.
-- The map specifies old column names to new column names.
renameColumns :: Map T.Text T.Text -> DataFrame -> DataFrame
renameColumns nameMap (DataFrame dfMap) =
    let
        newDfMap = Map.foldlWithKey (\accMap oldName newName ->
            case Map.lookup oldName accMap of
                Just col -> Map.insert newName col (Map.delete oldName accMap)
                Nothing -> accMap
            ) dfMap nameMap
    in
        DataFrame newDfMap

-- | Drops rows that contain any NA values.
dropNA :: DataFrame -> DataFrame
dropNA df =
    let
        rows = toRows df
        filteredRows = filter (not . any isNA . Map.elems) rows
        isNA NA = True
        isNA _ = False
        -- Convert filtered rows back to DataFrame
        newDfMap = if null filteredRows
                   then Map.empty
                   else
                       let
                           colNames = Map.keys (head filteredRows)
                           cols = [ V.fromList [ Map.findWithDefault NA colName r | r <- filteredRows ]
                                  | colName <- colNames
                                  ]
                       in
                           Map.fromList (zip colNames cols)
    in
        DataFrame newDfMap

-- | Replaces NA values with a specified DFValue.
fillNA :: DFValue -> DataFrame -> DataFrame
fillNA replacementValue (DataFrame dfMap) =
    let
        newDfMap = Map.map (V.map (\val -> if val == NA then replacementValue else val)) dfMap
    in
        DataFrame newDfMap