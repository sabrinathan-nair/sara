# Sara User Guide

Welcome to Sara! This guide is designed to help you understand and use Sara for your data manipulation tasks, even if you're new to Haskell or data processing.

## 1. Sara Explained: For a 5-Year-Old

Imagine you have a big box of LEGOs, but instead of random bricks, each LEGO piece is a piece of information, like a name, an age, or a city.

Sara is like a super-smart LEGO sorter and builder.

*   **DataFrames (Your LEGO Creations):** In Sara, we don't call them LEGO creations; we call them "DataFrames." A DataFrame is just an organized table of information, like a spreadsheet. Each column in your table has a specific type of LEGO brick (e.g., all names are text LEGOs, all ages are number LEGOs).

*   **Type Safety (No Mismatched LEGOs!):** This is Sara's superpower! Imagine your LEGO sorter *knows* that a square red brick can only connect to another square red brick, and a round blue brick only to a round blue brick. If you try to connect a square red brick to a round blue brick, the sorter immediately says, "Nope! That won't fit!"

    In Sara, this means:
    *   If you have a column of "ages" (which are numbers), Sara won't let you accidentally try to do math on a "name" column (which is text).
    *   If you ask for a column called "City" but you accidentally typed "Cty," Sara will tell you *before* you even run your program that "Cty" doesn't exist.

    This "no mismatched LEGOs" rule means your data programs are much less likely to break unexpectedly when you run them. Sara catches many mistakes *before* they happen!

*   **Streaming (Building with Endless LEGOs):** What if you have so many LEGOs that they don't all fit on your table at once? Sara can handle this! It's like Sara can build with LEGOs as they come out of an endless conveyor belt, without needing to see all of them at once. This is super useful for really, really big datasets.

## 2. Task-Oriented Guides

This section will provide step-by-step instructions for common data manipulation tasks.

### How to Load Data (e.g., from CSV, JSON)

Loading data into Sara is the first step to performing any analysis. Sara can read data from various sources, such as CSV (Comma Separated Values) and JSON (JavaScript Object Notation) files.

#### Loading from a CSV File

To load data from a CSV file, you typically use a function like `readCsv`. Let's say you have a file named `people.csv` with the following content:

```csv
Name,Age,City
Alice,30,New York
Bob,24,London
Charlie,35,Paris
```

You can load this into a Sara DataFrame like this:

```haskell
-- Assuming 'readCsv' is available in your Sara environment
-- and 'people.csv' is in the correct path.
myDataFrame <- readCsv "people.csv"
```

After running this, `myDataFrame` will hold your data, and Sara will have automatically inferred the types for each column (e.g., `Name` as Text, `Age` as Int, `City` as Text).

#### Loading from a JSON File

Similarly, to load data from a JSON file, you would use a function like `readJson`. If you have a file named `people.json` with content like this:

```json
[
  {"Name": "Alice", "Age": 30, "City": "New York"},
  {"Name": "Bob", "Age": 24, "City": "London"},
  {"Name": "Charlie", "Age": 35, "City": "Paris"}
]
```

You can load it into a Sara DataFrame:

```haskell
-- Assuming 'readJson' is available in your Sara environment
-- and 'people.json' is in the correct path.
myDataFrame <- readJson "people.json"
```

Sara will again infer the schema based on the JSON structure.

### How to Filter Rows

Filtering rows in a DataFrame allows you to select only the data that meets certain conditions. This is like picking out only the red LEGO bricks from your big box.

Sara's type safety ensures that you can only apply filters that make sense for the data type of the column. For example, you can't check if a 'Name' (Text) is greater than 10.

Let's use our `myDataFrame` from the previous section:

```haskell
-- myDataFrame:
-- Name    Age  City
-- Alice   30   New York
-- Bob     24   London
-- Charlie 35   Paris
```

#### Filtering by a Numeric Column

To filter rows where the 'Age' is greater than 25:

```haskell
import Sara.DataFrame.Predicate ((.>), (.>=), (.<), (.<=), (.==), (.!=))
import Sara.DataFrame.Wrangling (filterRows)
import Sara.DataFrame.Static (C) -- For column references

-- Filter for people older than 25
olderPeopleDF <- myDataFrame & filterRows (C "Age" .> 25)
-- Result:
-- Name    Age  City
-- Alice   30   New York
-- Charlie 35   Paris
```

Here:
*   `C "Age"` refers to the 'Age' column.
*   `.>` is the "greater than" operator for columns. Sara provides similar operators like `.>=` (greater than or equal), `.<` (less than), `.<=` (less than or equal), `.==` (equal), and `.!=` (not equal).

#### Filtering by a Text Column

To filter rows where the 'City' is 'New York':

```haskell
-- Filter for people living in New York
nyResidentsDF <- myDataFrame & filterRows (C "City" .== "New York")
-- Result:
-- Name    Age  City
-- Alice   30   New York
```

#### Combining Filters

You can combine multiple conditions using logical operators like `&&.` (AND) and `||.` (OR):

```haskell
-- Filter for people older than 25 AND living in New York
filteredDF <- myDataFrame & filterRows ((C "Age" .> 25) &&. (C "City" .== "New York"))
-- Result:
-- Name    Age  City
-- Alice   30   New York

-- Filter for people younger than 30 OR living in London
anotherFilteredDF <- myDataFrame & filterRows ((C "Age" .< 30) ||. (C "City" .== "London"))
-- Result:
-- Name    Age  City
-- Bob     24   London
```

### How to Select and Drop Columns

Selecting and dropping columns allows you to focus on specific parts of your DataFrame, much like picking out only the blue and green LEGO bricks, or removing all the yellow ones.

Let's continue with our `myDataFrame`:

```haskell
-- myDataFrame:
-- Name    Age  City
-- Alice   30   New York
-- Bob     24   London
-- Charlie 35   Paris
```

#### Selecting Columns

To select only the 'Name' and 'Age' columns:

```haskell
import Sara.DataFrame.Wrangling (selectColumns)
import Sara.DataFrame.Static (C) -- For column references

selectedDF <- myDataFrame & selectColumns (C "Name" :*: C "Age" :*: HNil)
-- Result:
-- Name    Age
-- Alice   30
-- Bob     24
-- Charlie 35
```

Here:
*   `selectColumns` is the function used to pick specific columns.
*   `C "Name" :*: C "Age" :*: HNil` is how you list the columns you want to select. `HNil` marks the end of the list.

#### Dropping Columns

To remove the 'City' column from your DataFrame:

```haskell
import Sara.DataFrame.Wrangling (dropColumns)
import Sara.DataFrame.Static (C) -- For column references

droppedDF <- myDataFrame & dropColumns (C "City" :*: HNil)
-- Result:
-- Name    Age
-- Alice   30
-- Bob     24
-- Charlie 35
```

Here:
*   `dropColumns` is the function used to remove specific columns.
*   `C "City" :*: HNil` lists the columns you want to drop.

### How to Join DataFrames

Joining DataFrames allows you to combine two DataFrames based on common columns, much like merging two separate lists of LEGOs that share some common identifier.

Let's imagine we have two DataFrames:

`employeesDF`:
```
Name    EmployeeID  Department
Alice   101         HR
Bob     102         Engineering
Charlie 103         Marketing
```

`salariesDF`:
```
EmployeeID  Salary
101         70000
102         90000
103         80000
```

We want to combine these two DataFrames to see each employee's department and salary.

#### Inner Join

An inner join returns only the rows where there is a match in *both* DataFrames based on the specified common column(s). In our LEGO analogy, it's like only keeping the LEGO pieces that have a matching connector on both sides.

```haskell
import Sara.DataFrame.Join (joinDF, JoinType(InnerJoin))
import Sara.DataFrame.Static (C)

-- Join employeesDF and salariesDF on the 'EmployeeID' column
joinedDF <- joinDF InnerJoin (C "EmployeeID") employeesDF salariesDF
-- Result:
-- Name    EmployeeID  Department  Salary
-- Alice   101         HR          70000
-- Bob     102         Engineering 90000
-- Charlie 103         Marketing   80000
```

Here:
*   `joinDF` is the function for joining DataFrames.
*   `InnerJoin` specifies the type of join.
*   `C "EmployeeID"` indicates the common column to join on.

#### Other Join Types

Sara typically supports other standard SQL join types like `LeftJoin`, `RightJoin`, and `FullJoin`. The usage would be similar, just by changing the `JoinType` argument.

### How to Aggregate Data (e.g., sum, average)

Aggregating data means performing calculations on groups of rows to produce a single summary value, like finding the total sum of a column, the average, or counting items. This is like counting how many red LEGO bricks you have, or finding the average height of all your LEGO towers.

Let's use a DataFrame with sales data:

`salesDF`:
```
Region    Product   Sales
East      A         100
East      B         150
West      A         200
West      C         50
East      A         120
```

#### Sum Aggregation

To find the total sales for all products:

```haskell
import Sara.DataFrame.Aggregate (sumAgg)
import Sara.DataFrame.Static (C)

totalSales <- salesDF & sumAgg (C "Sales")
-- Result: 620 (a single numeric value)
```

To find the total sales per region (group by 'Region'):

```haskell
import Sara.DataFrame.Aggregate (groupBy, sumAgg)
import Sara.DataFrame.Static (C)

salesByRegion <- salesDF & groupBy (C "Region") (sumAgg (C "Sales"))
-- Result (conceptual DataFrame):
-- Region  TotalSales
-- East    370
-- West    250
```

#### Average Aggregation

To find the average sales for all products:

```haskell
import Sara.DataFrame.Aggregate (meanAgg)
import Sara.DataFrame.Static (C)

averageSales <- salesDF & meanAgg (C "Sales")
-- Result: 124.0 (a single numeric value)
```

To find the average sales per product:

```haskell
import Sara.DataFrame.Aggregate (groupBy, meanAgg)
import Sara.DataFrame.Static (C)

averageSalesByProduct <- salesDF & groupBy (C "Product") (meanAgg (C "Sales"))
-- Result (conceptual DataFrame):
-- Product  AverageSales
-- A        140.0
-- B        150.0
-- C        50.0
```

#### Count Aggregation

To count the number of sales records:

```haskell
import Sara.DataFrame.Aggregate (countAgg)

recordCount <- salesDF & countAgg
-- Result: 5 (a single numeric value)
```

To count the number of products sold per region:

```haskell
import Sara.DataFrame.Aggregate (groupBy, countAgg)
import Sara.DataFrame.Static (C)

productCountByRegion <- salesDF & groupBy (C "Region") countAgg
-- Result (conceptual DataFrame):
-- Region  Count
-- East    3
-- West    2
```

### How to Count Value Occurrences

Counting the occurrences of unique values in a column is a common statistical operation, useful for understanding the distribution of categorical data or identifying frequent items. Sara provides the `countValues` function for this purpose.

Let's use a DataFrame representing survey responses:

`surveyDF`:
```
ID  Response
1   Yes
2   No
3   Yes
4   Maybe
5   Yes
6   No
```

To count the occurrences of each unique 'Response':

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.Static (C)
import Sara.DataFrame.Statistics (countValues)
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

main :: IO ()
main = do
  let surveyRows = [
          Map.fromList [("ID", IntValue 1), ("Response", TextValue "Yes")],
          Map.fromList [("ID", IntValue 2), ("Response", TextValue "No")],
          Map.fromList [("ID", IntValue 3), ("Response", TextValue "Yes")],
          Map.fromList [("ID", IntValue 4), ("Response", TextValue "Maybe")],
          Map.fromList [("ID", IntValue 5), ("Response", TextValue "Yes")],
          Map.fromList [("ID", IntValue 6), ("Response", TextValue "No")]
          ]
  let surveyDF = fromRows @'[ '("ID", Int), '("Response", T.Text)] surveyRows

  putStrLn "\nOriginal Survey DataFrame:"
  print surveyDF

  case countValues (Proxy @"Response") surveyDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right countsDF -> do
      putStrLn "\nValue Counts for 'Response':"
      print countsDF

  putStrLn "\nAnalysis Complete."
```

**Expected Output (conceptual):**
```
Response  Count
Yes       3
No        2
Maybe     1
```

This demonstrates how `countValues` provides a quick summary of the frequency of each unique item in a column, sorted by their counts in descending order.

### How to Calculate Quantiles

Quantiles (also known as percentiles) are points in a distribution that divide the data into equal-sized groups. For example, the 0.5 quantile is the median, dividing the data into two halves. The 0.25 and 0.75 quantiles (Q1 and Q3) divide the data into four quarters.

Sara's `quantile` function allows you to calculate these values for any numeric column. It handles missing values (`NA`) by ignoring them.

Let's use a DataFrame with student scores:

`scoresDF`:
```
Student  Score
Alice    85
Bob      92
Charlie  78
David    95
Eve      88
Frank    70
Grace    80
```

To calculate the median (0.5 quantile) of the 'Score' column:

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.Static (C)
import Sara.DataFrame.Statistics (quantile)
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

main :: IO ()
main = do
  let scoreRows = [
          Map.fromList [("Student", TextValue "Alice"), ("Score", IntValue 85)],
          Map.fromList [("Student", TextValue "Bob"), ("Score", IntValue 92)],
          Map.fromList [("Student", TextValue "Charlie"), ("Score", IntValue 78)],
          Map.fromList [("Student", TextValue "David"), ("Score", IntValue 95)],
          Map.fromList [("Student", TextValue "Eve"), ("Score", IntValue 88)],
          Map.fromList [("Student", TextValue "Frank"), ("Score", IntValue 70)],
          Map.fromList [("Student", TextValue "Grace"), ("Score", IntValue 80)]
          ]
  let scoresDF = fromRows @'[ '("Student", T.Text), '("Score", Int)] scoreRows

  putStrLn "\nOriginal Scores DataFrame:"
  print scoresDF

  -- Calculate the median (0.5 quantile)
  case quantile (Proxy @"Score") 0.5 scoresDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right medianVal -> putStrLn $ "\nMedian Score: " ++ show medianVal

  -- Calculate the 0.25 quantile (Q1)
  case quantile (Proxy @"Score") 0.25 scoresDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right q1Val -> putStrLn $ "Q1 Score: " ++ show q1Val

  -- Calculate the 0.75 quantile (Q3)
  case quantile (Proxy @"Score") 0.75 scoresDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right q3Val -> putStrLn $ "Q3 Score: " ++ show q3Val

  putStrLn "\nAnalysis Complete."
```

**Expected Output (conceptual):**
```
Original Scores DataFrame:
...

Median Score: DoubleValue 85.0
Q1 Score: DoubleValue 79.0
Q3 Score: DoubleValue 90.0

Analysis Complete.
```

This example demonstrates how to use `quantile` to get key statistical measures of your data's distribution.

### How to Calculate Percentiles

Percentiles are very similar to quantiles, but instead of a fraction (0.0 to 1.0), they use a percentage (0 to 100). The `percentile` function in Sara allows you to find the value below which a given percentage of observations fall.

Let's use the same `scoresDF` from the quantile example:

`scoresDF`:
```
Student  Score
Alice    85
Bob      92
Charlie  78
David    95
Eve      88
Frank    70
Grace    80
```

To find the 90th percentile of the 'Score' column (the score below which 90% of students fall):

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.Static (C)
import Sara.DataFrame.Statistics (percentile)
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

main :: IO ()
main = do
  let scoreRows = [
          Map.fromList [("Student", TextValue "Alice"), ("Score", IntValue 85)],
          Map.fromList [("Student", TextValue "Bob"), ("Score", IntValue 92)],
          Map.fromList [("Student", TextValue "Charlie"), ("Score", IntValue 78)],
          Map.fromList [("Student", TextValue "David"), ("Score", IntValue 95)],
          Map.fromList [("Student", TextValue "Eve"), ("Score", IntValue 88)],
          Map.fromList [("Student", TextValue "Frank"), ("Score", IntValue 70)],
          Map.fromList [("Student", TextValue "Grace"), ("Score", IntValue 80)]
          ]
  let scoresDF = fromRows @'[ '("Student", T.Text), '("Score", Int)] scoreRows

  putStrLn "\nOriginal Scores DataFrame:"
  print scoresDF

  -- Calculate the 90th percentile
  case percentile (Proxy @"Score") 90.0 scoresDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right p90Val -> putStrLn $ "\n90th Percentile Score: " ++ show p90Val

  -- Calculate the 10th percentile
  case percentile (Proxy @"Score") 10.0 scoresDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right p10Val -> putStrLn $ "10th Percentile Score: " ++ show p10Val

  putStrLn "\nAnalysis Complete."
```

**Expected Output (conceptual):**
```
Original Scores DataFrame:
...

90th Percentile Score: DoubleValue 93.0
10th Percentile Score: DoubleValue 72.0

Analysis Complete.
```

This example shows how to use `percentile` to understand the spread of your data in terms of percentages.

### How to Calculate Correlation

Correlation measures the statistical relationship between two numeric columns. The Pearson correlation coefficient, which Sara's `correlate` function calculates, ranges from -1 to +1:

*   **+1:** Perfect positive linear relationship (as one variable increases, the other increases proportionally).
*   **-1:** Perfect negative linear relationship (as one variable increases, the other decreases proportionally).
*   **0:** No linear relationship.

Sara's `correlate` function takes two column proxies and a DataFrame, returning the correlation coefficient or an error if the columns are not found, are not numeric, or have insufficient data.

Let's use a DataFrame with student data, including their study hours and exam scores:

`studentDataDF`:
```
Student  StudyHours  ExamScore
Alice    10          85
Bob      12          92
Charlie  8           78
David    15          95
Eve      11          88
Frank    7           70
Grace    9           80
```

To calculate the correlation between 'StudyHours' and 'ExamScore':

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.Static (C)
import Sara.DataFrame.Statistics (correlate)
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

main :: IO ()
main = do
  let studentRows = [
          Map.fromList [("Student", TextValue "Alice"), ("StudyHours", IntValue 10), ("ExamScore", IntValue 85)],
          Map.fromList [("Student", TextValue "Bob"), ("StudyHours", IntValue 12), ("ExamScore", IntValue 92)],
          Map.fromList [("Student", TextValue "Charlie"), ("StudyHours", IntValue 8), ("ExamScore", IntValue 78)],
          Map.fromList [("Student", TextValue "David"), ("StudyHours", IntValue 15), ("ExamScore", IntValue 95)],
          Map.fromList [("Student", TextValue "Eve"), ("StudyHours", IntValue 11), ("ExamScore", IntValue 88)],
          Map.fromList [("Student", TextValue "Frank"), ("StudyHours", IntValue 7), ("ExamScore", IntValue 70)],
          Map.fromList [("Student", TextValue "Grace"), ("StudyHours", IntValue 9), ("ExamScore", IntValue 80)]
          ]
  let studentDataDF = fromRows @'[ '("Student", T.Text), '("StudyHours", Int), '("ExamScore", Int)] studentRows

  putStrLn "\nOriginal Student Data DataFrame:"
  print studentDataDF

  -- Calculate the correlation between StudyHours and ExamScore
  case correlate (Proxy @"StudyHours") (Proxy @"ExamScore") studentDataDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right correlation -> putStrLn $ "\nCorrelation between StudyHours and ExamScore: " ++ show correlation

  putStrLn "\nAnalysis Complete."
```

**Expected Output (conceptual):**
```
Original Student Data DataFrame:
...

Correlation between StudyHours and ExamScore: 0.98...

Analysis Complete.
```

This example demonstrates how to use `correlate` to understand the linear relationship between two numeric variables in your DataFrame.

### How to Calculate Covariance

Covariance measures how two variables change together. A positive covariance indicates that the variables tend to move in the same direction, while a negative covariance indicates they tend to move in opposite directions. A covariance of zero suggests no linear relationship.

Sara's `covariance` function takes two column proxies and a DataFrame, returning the covariance or an error if the columns are not found, are not numeric, or have insufficient data.

Let's use the same `studentDataDF` from the correlation example:

`studentDataDF`:
```
Student  StudyHours  ExamScore
Alice    10          85
Bob      12          92
Charlie  8           78
David    15          95
Eve      11          88
Frank    7           70
Grace    9           80
```

To calculate the covariance between 'StudyHours' and 'ExamScore':

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.Static (C)
import Sara.DataFrame.Statistics (covariance)
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

main :: IO () 
main = do
  let studentRows = [
          Map.fromList [("Student", TextValue "Alice"), ("StudyHours", IntValue 10), ("ExamScore", IntValue 85)],
          Map.fromList [("Student", TextValue "Bob"), ("StudyHours", IntValue 12), ("ExamScore", IntValue 92)],
          Map.fromList [("Student", TextValue "Charlie"), ("StudyHours", IntValue 8), ("ExamScore", IntValue 78)],
          Map.fromList [("Student", TextValue "David"), ("StudyHours", IntValue 15), ("ExamScore", IntValue 95)],
          Map.fromList [("Student", TextValue "Eve"), ("StudyHours", IntValue 11), ("ExamScore", IntValue 88)],
          Map.fromList [("Student", TextValue "Frank"), ("StudyHours", IntValue 7), ("ExamScore", IntValue 70)],
          Map.fromList [("Student", TextValue "Grace"), ("StudyHours", IntValue 9), ("ExamScore", IntValue 80)]
          ]
  let studentDataDF = fromRows @'[ '("Student", T.Text), '("StudyHours", Int), '("ExamScore", Int)] studentRows

  putStrLn "\nOriginal Student Data DataFrame:"
  print studentDataDF

  -- Calculate the covariance between StudyHours and ExamScore
  case covariance (Proxy @"StudyHours") (Proxy @"ExamScore") studentDataDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right cov -> putStrLn $ "\nCovariance between StudyHours and ExamScore: " ++ show cov

  putStrLn "\nAnalysis Complete."
```

**Expected Output (conceptual):**
```
Original Student Data DataFrame:
...

Covariance between StudyHours and ExamScore: 12.0

Analysis Complete.
```

This example demonstrates how to use `covariance` to understand how two numeric variables in your DataFrame vary together.

### How to Get Summary Statistics

Summary statistics provide a quick overview of the central tendency, dispersion, and shape of a dataset's distribution. Sara's `summaryStatistics` function calculates common descriptive statistics for all numeric columns in your DataFrame.

The output is a `Map` where the top-level keys are the statistic names (e.g., "count", "mean", "std", "min", "25%", "50%", "75%", "max"), and the values are another `Map` from column names to their calculated statistic.

Let's use a DataFrame with sales data:

`salesDataDF`:
```
Product   Price  Quantity
Laptop    1200   5
Mouse     25     20
Keyboard  75     10
Monitor   300    3
Webcam    50     15
```

To get summary statistics for the numeric columns ('Price' and 'Quantity'):

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.Static (C)
import Sara.DataFrame.Statistics (summaryStatistics)
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

main :: IO ()
main = do
  let salesRows = [
          Map.fromList [("Product", TextValue "Laptop"), ("Price", IntValue 1200), ("Quantity", IntValue 5)],
          Map.fromList [("Product", TextValue "Mouse"), ("Price", IntValue 25), ("Quantity", IntValue 20)],
          Map.fromList [("Product", TextValue "Keyboard"), ("Price", IntValue 75), ("Quantity", IntValue 10)],
          Map.fromList [("Product", TextValue "Monitor"), ("Price", IntValue 300), ("Quantity", IntValue 3)],
          Map.fromList [("Product", TextValue "Webcam"), ("Price", IntValue 50), ("Quantity", IntValue 15)]
          ]
  let salesDataDF = fromRows @'[ '("Product", T.Text), '("Price", Int), '("Quantity", Int)] salesRows

  putStrLn "\nOriginal Sales Data DataFrame:"
  print salesDataDF

  let stats = summaryStatistics salesDataDF

  putStrLn "\nSummary Statistics:"
  Map.traverseWithKey (\statName colStats -> do
    putStrLn $ T.unpack statName ++ ":"
    Map.traverseWithKey (\colName statVal ->
      putStrLn $ "  " ++ T.unpack colName ++ ": " ++ show statVal
    ) colStats
  ) stats

  putStrLn "\nAnalysis Complete."
```

**Expected Output (conceptual):**
```
Original Sales Data DataFrame:
...

Summary Statistics:
25%:
  Price: DoubleValue 50.0
  Quantity: DoubleValue 5.0
50%:
  Price: DoubleValue 75.0
  Quantity: DoubleValue 10.0
75%:
  Price: DoubleValue 300.0
  Quantity: DoubleValue 15.0
count:
  Price: IntValue 5
  Quantity: IntValue 5
max:
  Price: DoubleValue 1200.0
  Quantity: DoubleValue 20.0
mean:
  Price: DoubleValue 330.0
  Quantity: DoubleValue 10.6
min:
  Price: DoubleValue 25.0
  Quantity: DoubleValue 3.0
std:
  Price: DoubleValue 509.95...
  Quantity: DoubleValue 6.73...

Analysis Complete.
```

This example demonstrates how to use `summaryStatistics` to quickly get a comprehensive overview of your numeric data.

### How to Apply Expanding Window Functions

Expanding window functions apply an aggregation over all data points from the beginning of the series up to the current point. This is useful for calculating cumulative sums, running averages that consider all prior data, etc.

Sara's `expandingApply` function takes a column proxy and an aggregation function. It returns a new DataFrame with an additional column containing the expanding window results.

Let's use a DataFrame with daily sales figures:

`dailySalesDF`:
```
Day  Sales
1    100
2    120
3    110
4    130
5    105
```

To calculate the cumulative sum of 'Sales':

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.Static (C)
import Sara.DataFrame.Statistics (expandingApply, sumV)
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import qualified Data.Vector as V

main :: IO ()
main = do
  let salesRows = [
          Map.fromList [("Day", IntValue 1), ("Sales", IntValue 100)],
          Map.fromList [("Day", IntValue 2), ("Sales", IntValue 120)],
          Map.fromList [("Day", IntValue 3), ("Sales", IntValue 110)],
          Map.fromList [("Day", IntValue 4), ("Sales", IntValue 130)],
          Map.fromList [("Day", IntValue 5), ("Sales", IntValue 105)]
          ]
  let dailySalesDF = fromRows @'[ '("Day", Int), '("Sales", Int)] salesRows

  putStrLn "\nOriginal Daily Sales DataFrame:"
  print dailySalesDF

  case expandingApply (Proxy @"Sales") sumV dailySalesDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right cumulativeSalesDF -> do
      putStrLn "\nCumulative Sales (expanding sum):"
      print cumulativeSalesDF

  putStrLn "\nAnalysis Complete."
```

**Expected Output (conceptual):**
```
Original Daily Sales DataFrame:
...

Cumulative Sales (expanding sum):
Day  Sales  Sales_expanding
1    100    100.0
2    120    220.0
3    110    330.0
4    130    460.0
5    105    565.0

Analysis Complete.
```

This example demonstrates how `expandingApply` can be used to calculate cumulative metrics over your data.

### How to Apply Exponentially Weighted Moving Functions

Exponentially Weighted Moving Average (EWMA) gives more weight to recent observations, making it suitable for smoothing time series data and reacting more quickly to recent changes than a simple moving average. The `ewmApply` function calculates the EWMA for a numeric column.

It requires an `alpha` (smoothing factor) between 0.0 and 1.0 (exclusive). A higher `alpha` gives more weight to recent data.

Let's use a DataFrame with stock prices:

`stockPricesDF`:
```
Date        Price
2023-01-01  100.0
2023-01-02  102.0
2023-01-03  101.5
2023-01-04  103.0
2023-01-05  102.5
```

To calculate the EWMA of 'Price' with an `alpha` of 0.3:

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.Static (C)
import Sara.DataFrame.Statistics (ewmApply)
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import qualified Data.Vector as V

main :: IO ()
main = do
  let priceRows = [
          Map.fromList [("Date", TextValue "2023-01-01"), ("Price", DoubleValue 100.0)],
          Map.fromList [("Date", TextValue "2023-01-02"), ("Price", DoubleValue 102.0)],
          Map.fromList [("Date", TextValue "2023-01-03"), ("Price", DoubleValue 101.5)],
          Map.fromList [("Date", TextValue "2023-01-04"), ("Price", DoubleValue 103.0)],
          Map.fromList [("Date", TextValue "2023-01-05"), ("Price", DoubleValue 102.5)]
          ]
  let stockPricesDF = fromRows @'[ '("Date", T.Text), '("Price", Double)] priceRows

  putStrLn "\nOriginal Stock Prices DataFrame:"
  print stockPricesDF

  case ewmApply (Proxy @"Price") 0.3 stockPricesDF of
    Left err -> putStrLn $ "Error: " ++ show err
    Right ewmaDF -> do
      putStrLn "\nExponentially Weighted Moving Average (alpha=0.3):"
      print ewmaDF

  putStrLn "\nAnalysis Complete."
```

**Expected Output (conceptual):**
```
Original Stock Prices DataFrame:
...

Exponentially Weighted Moving Average (alpha=0.3):
Date        Price  Price_ewma
2023-01-01  100.0  100.0
2023-01-02  102.0  100.6
2023-01-03  101.5  100.97
2023-01-04  103.0  101.679
2023-01-05  102.5  101.9753

Analysis Complete.
```

This example demonstrates how `ewmApply` can be used for smoothing time series data.

## 4. IDE Configuration for Haskell Development








Aggregating data means performing calculations on groups of rows to produce a single summary value, like finding the total sum of a column, the average, or counting items. This is like counting how many red LEGO bricks you have, or finding the average height of all your LEGO towers.

Let's use a DataFrame with sales data:

`salesDF`:
```
Region    Product   Sales
East      A         100
East      B         150
West      A         200
West      C         50
East      A         120
```

#### Sum Aggregation

To find the total sales for all products:

```haskell
import Sara.DataFrame.Aggregate (sumAgg)
import Sara.DataFrame.Static (C)

totalSales <- salesDF & sumAgg (C "Sales")
-- Result: 620 (a single numeric value)
```

To find the total sales per region (group by 'Region'):

```haskell
import Sara.DataFrame.Aggregate (groupBy, sumAgg)
import Sara.DataFrame.Static (C)

salesByRegion <- salesDF & groupBy (C "Region") (sumAgg (C "Sales"))
-- Result (conceptual DataFrame):
-- Region  TotalSales
-- East    370
-- West    250
```

#### Average Aggregation

To find the average sales for all products:

```haskell
import Sara.DataFrame.Aggregate (meanAgg)
import Sara.DataFrame.Static (C)

averageSales <- salesDF & meanAgg (C "Sales")
-- Result: 124.0 (a single numeric value)
```

To find the average sales per product:

```haskell
import Sara.DataFrame.Aggregate (groupBy, meanAgg)
import Sara.DataFrame.Static (C)

averageSalesByProduct <- salesDF & groupBy (C "Product") (meanAgg (C "Sales"))
-- Result (conceptual DataFrame):
-- Product  AverageSales
-- A        140.0
-- B        150.0
-- C        50.0
```

#### Count Aggregation

To count the number of sales records:

```haskell
import Sara.DataFrame.Aggregate (countAgg)

recordCount <- salesDF & countAgg
-- Result: 5 (a single numeric value)
```

To count the number of products sold per region:

```haskell
import Sara.DataFrame.Aggregate (groupBy, countAgg)
import Sara.DataFrame.Static (C)

productCountByRegion <- salesDF & groupBy (C "Region") countAgg
-- Result (conceptual DataFrame):
-- Region  Count
-- East    3
-- West    2
```

## 4. IDE Configuration for Haskell Development

Developing with Sara, like any Haskell project, greatly benefits from a well-configured Integrated Development Environment (IDE) or text editor. The key to a good Haskell development experience across most editors is the **Haskell Language Server (HLS)**. HLS provides features like:

*   **Code Completion:** Suggestions for functions, types, and variables.
*   **Type Information:** Hover over code to see its inferred type.
*   **Error and Warning Diagnostics:** Real-time feedback on compilation issues.
*   **Code Formatting:** Integration with tools like `ormolu` or `stylish-haskell`.
*   **Refactoring Tools:** Basic refactoring capabilities.

HLS works by integrating with your build system (Cabal or Stack) to understand your project.

Here's how to set up some popular IDEs/editors for Haskell development, which will in turn provide excellent support for Sara:

### Visual Studio Code (VS Code)

VS Code is a popular choice due to its rich extension ecosystem.

1.  **Install VS Code:** If you don't have it, download and install it from [code.visualstudio.com](https://code.visualstudio.com/).
2.  **Install Haskell Extension:** Open VS Code, go to the Extensions view (Ctrl+Shift+X or Cmd+Shift+X), and search for "Haskell". Install the official "Haskell" extension by Haskell.
3.  **Install HLS:** The Haskell VS Code extension will usually prompt you to install `haskell-language-server` (HLS) if it's not found. Follow the prompts. You can also install it manually via `ghcup`:
    ```bash
    ghcup install hls
    ghcup set hls
    ```
4.  **Open Sara Project:** Open the root directory of your Sara project (`numerology-hs`) in VS Code. HLS should automatically detect your Cabal project and start providing language features.

### Vim / Neovim

For Vim and Neovim users, `coc.nvim` (Conquer of Completion) is a popular choice for integrating Language Server Protocol (LSP) features.

1.  **Install `coc.nvim`:** Follow the installation instructions for `coc.nvim` (e.g., using `Plug 'neoclide/coc.nvim', {'branch': 'release'}`).
2.  **Install `coc-tsserver` (for JavaScript/TypeScript, if needed) and `coc-json`:**
    ```vim
    :CocInstall coc-tsserver coc-json
    ```
3.  **Install HLS:** Ensure `haskell-language-server` is installed via `ghcup` (see VS Code instructions above).
4.  **Configure `coc-settings.json`:** Add a language server configuration for Haskell. You can open `coc-settings.json` with `:CocConfig` and add something like:
    ```json
    {
      "languageserver": {
        "haskell": {
          "command": "haskell-language-server-wrapper",
          "args": [ "--lsp" ],
          "rootPatterns": [
            "*.cabal",
            "stack.yaml",
            "cabal.project",
            ".git/"
          ],
          "filetypes": [ "haskell", "lhaskell" ]
        }
      }
    }
    ```
    This tells `coc.nvim` to use `haskell-language-server-wrapper` for Haskell files.

### Emacs

Emacs users can leverage `lsp-mode` for LSP integration.

1.  **Install `lsp-mode`:** Use `package-install` to install `lsp-mode` and `dap-mode`.
2.  **Install HLS:** Ensure `haskell-language-server` is installed via `ghcup`.
3.  **Configure `init.el`:** Add the following to your Emacs configuration:
    ```emacs-lisp
    (require 'lsp-mode)
    (add-hook 'haskell-mode-hook #'lsp)
    (setq lsp-haskell-server-path "haskell-language-server-wrapper")
    (setq lsp-haskell-server-args '("--lsp"))
    ```
    You might also want to install `haskell-mode` for basic Haskell editing features.

### JetBrains IntelliJ (with Haskell Plugin)

JetBrains IDEs offer robust support through plugins.

1.  **Install IntelliJ IDEA (Ultimate Edition recommended for full features):** Download from [jetbrains.com/idea/](https://www.jetbrains.com/idea/).
2.  **Install Haskell Plugin:** Go to `File > Settings/Preferences > Plugins`, search for "Haskell", and install the "Haskell" plugin.
3.  **Configure Plugin:** The plugin will guide you through setting up GHC, Cabal, and HLS. Ensure HLS is installed via `ghcup` as described above.

### Sublime Text

Sublime Text can use LSP features via the `LSP` package.

1.  **Install Package Control:** If you don't have it, install Package Control for Sublime Text.
2.  **Install `LSP` Package:** Open the Command Palette (Ctrl+Shift+P or Cmd+Shift+P), select `Package Control: Install Package`, and search for "LSP".
3.  **Install `LSP-haskell` Package:** After installing `LSP`, install `LSP-haskell` through Package Control.
4.  **Install HLS:** Ensure `haskell-language-server` is installed via `ghcup`.
5.  **Configure `LSP-haskell`:** You might need to configure `LSP-haskell` settings (e.g., `Preferences > Package Settings > LSP > LSP-haskell > Settings`) to point to your HLS executable if it's not found automatically.

### Atom (Deprecated) and Other Editors

While Atom was once popular, it is now deprecated. For other editors, the general approach remains the same:

1.  **Install an LSP client plugin/package** for your editor (if it doesn't have native LSP support).
2.  **Ensure `haskell-language-server` (HLS) is installed** and accessible in your system's PATH.
3.  **Configure the LSP client** to use `haskell-language-server-wrapper` as the command for Haskell files.

By following these steps, you should be able to get a productive development environment for Sara and other Haskell projects in your preferred editor.

## 3. Interactive Examples

This section provides runnable code snippets that combine various Sara operations. You can copy and paste these into your Haskell environment (e.g., a `.hs` file or a GHCi session) to experiment.

#### Example: Analyze Employee Data

Let's imagine we have a CSV file named `employees.csv` with the following content:

```csv
Name,Department,Salary,YearsExperience
Alice,HR,70000,5
Bob,Engineering,90000,8
Charlie,Marketing,80000,6
David,HR,60000,3
Eve,Engineering,95000,10
```

We want to perform the following analysis:
1.  Load the `employees.csv` file.
2.  Filter for employees in the 'Engineering' department.
3.  Select only the 'Name' and 'Salary' columns.
4.  Calculate the average salary for the filtered employees.

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE OverloadedStrings #-}

import Sara.DataFrame
import Sara.DataFrame.IO (readCsv)
import Sara.DataFrame.Predicate ((.==))
import Sara.DataFrame.Wrangling (filterRows, selectColumns)
import Sara.DataFrame.Aggregate (meanAgg)
import Sara.DataFrame.Static (C, HNil, (:*:))

main :: IO ()
main = do
  -- 1. Load the employees.csv file
  employeesDF <- readCsv "employees.csv"

  putStrLn "\nOriginal DataFrame:"
  print employeesDF -- Assuming a print instance for DataFrame

  -- 2. Filter for employees in the 'Engineering' department
  engineeringDF <- employeesDF & filterRows (C "Department" .== "Engineering")

  putStrLn "\nEngineering Department Employees:"
  print engineeringDF

  -- 3. Select only the 'Name' and 'Salary' columns
  namesAndSalariesDF <- engineeringDF & selectColumns (C "Name" :*: C "Salary" :*: HNil)

  putStrLn "\nNames and Salaries (Engineering):"
  print namesAndSalariesDF

  -- 4. Calculate the average salary for the filtered employees
  averageEngSalary <- engineeringDF & meanAgg (C "Salary")

  putStrLn $ "\nAverage Engineering Salary: " ++ show averageEngSalary

  putStrLn "\nAnalysis Complete."
```

**To run this example:**

1.  Save the code above as a Haskell file (e.g., `AnalyzeEmployees.hs`).
2.  Make sure you have an `employees.csv` file in the same directory as your Haskell file, with the content provided above.
3.  Compile and run using Cabal or Stack (assuming Sara is installed and configured):
    ```bash
    cabal run AnalyzeEmployees.hs
    # or if using stack
    stack runghc AnalyzeEmployees.hs
    ```

This example demonstrates how Sara's type-safe operations guide you through data manipulation, catching potential errors at compile time.
