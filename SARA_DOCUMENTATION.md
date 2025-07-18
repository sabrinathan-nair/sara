# Sara's Secret Handbook: How Sara Works!

Welcome to Sara's secret handbook! Here, we'll learn about all the special parts that make Sara a super smart helper for your data. Think of your data like a big box of toys, and Sara helps you keep them organized and play with them nicely.

## 1. The Main Building Blocks (Sara.DataFrame.Types)

This part of Sara's brain knows all about the basic shapes and sizes of your toys. It helps Sara understand what each toy is and how they fit together.

## 2. Sara's Helper Friends (Module Summaries)

Sara has many helper friends, and each one is good at a special job. Let's meet them!

### `Sara.DataFrame.Aggregate`: The Counting and Grouping Friend

This friend helps you count your toys and put them into groups. Like, "How many red cars?" or "How many blue blocks?"

### `Sara.DataFrame.Concat`: The Sticking-Together Friend

This friend helps you stick different toy boxes (DataFrames) together, either side-by-side or one on top of the other.

### `Sara.DataFrame.Expression`: The Smart Calculator Friend

This friend helps Sara do math and make smart decisions about your toys. Like, "Is this car bigger than that truck?" or "What happens if I add two blocks together?"

### `Sara.DataFrame.Instances`: The Rule-Maker Friend

This friend helps Sara understand the special rules for different kinds of toys, especially when reading them from lists.

### `Sara.DataFrame.Internal`: The Secret Brain Friend

This friend works deep inside Sara's brain, helping it understand how your toys are stored and how to move them around.

### `Sara.DataFrame.IO`: The List-Reading and Writing Friend

This friend helps Sara read your toy lists from special papers (like CSV and JSON files) and write them back down when you're done playing.

### `Sara.DataFrame.Join`: The Matchmaker Friend

This friend helps Sara find matching toys in different boxes and put them together. Like, finding all the red cars that also have big wheels.

### `Sara.DataFrame.Missing`: The Missing Toy Friend

This friend helps Sara deal with toys that are missing from your box. It knows how to mark them as 'NA' (Not Available) and sometimes even fill them in.

### `Sara.DataFrame.Predicate`: The Rule-Checker Friend

This friend helps Sara check if your toys follow certain rules. Like, "Is this toy red?" or "Is this toy bigger than that one?"

### `Sara.DataFrame.SQL`: The Treasure Map Friend

This friend helps Sara find toys hidden in big treasure chests (SQL databases).

### `Sara.DataFrame.Static`: The Fortune Teller Friend

This friend helps Sara guess what kind of toys are on your list *before* you even start playing, so it can make sure everything is just right.

### `Sara.DataFrame.Statistics`: The Number Cruncher Friend

This friend loves to count and measure things about your toys, like how many, how big, or how small.

### `Sara.DataFrame.Strings`: The Wordy Friend

This friend helps Sara play with the names and words on your toys, like making them all capital letters or finding a special word.

### `Sara.DataFrame.TimeSeries`: The Time Traveler Friend

This friend helps Sara understand toys that change over time, like how many cars you have each day.

### `Sara.DataFrame.Transform`: The Toy Changer Friend

This friend helps Sara change your toys in smart ways, like making a toy bigger or giving it a new color.

### `Sara.DataFrame.Wrangling`: The Toy Organizer Friend

This friend helps Sara sort, clean, and organize your toys, making sure everything is in its right place.

### `Sara.REPL`: The Chatty Friend

This friend lets you talk to Sara and tell it what to do with your toys, one step at a time.

### `app/Main.hs`: The Boss Friend

This is the main friend who tells Sara what big jobs to do when you start playing.

### `app/Tutorial.hs`: The Learning Friend

This friend shows you how to play with Sara and all its cool tricks, step by step.

---

## 3. Sara's Toy Pieces (DFValue)

Imagine each toy has a little tag that tells you what it is. That's what `DFValue` is! It's a special tag that Sara puts on every piece of your data (like a number, a word, a date, or even if something is missing).

## 4. A Stack of Toys (Column)

Imagine you have a stack of the same kind of toys, like a stack of red cars. That's what a `Column` is! It's a neat stack of `DFValue`s (your toy tags) that are all the same kind.

## 5. The Big Toy Box (DataFrame)

This is the main toy box where all your toys are kept! It's like a big table with different columns for different kinds of toys (like 'color', 'size', 'shape'). Sara knows exactly what kind of toys are in each column, so it never gets confused.

## 6. A Single Row of Toys (Row)

Imagine one line of toys in your big toy box. That's a `Row`! It's like a list of all the toy tags for one line of toys.

## 7. Making Rows and Columns (toRows and fromRows)

-   **`toRows`:** This is like taking all the toys out of the big toy box and lining them up in rows.
-   **`fromRows`:** This is like putting all your toy rows back into the big toy box.

## 8. How to Line Up Your Toys (SortOrder)

When you line up your toys, you can do it in two ways:

-   **`Ascending`:** Smallest to biggest, like counting 1, 2, 3...
-   **`Descending`:** Biggest to smallest, like counting 3, 2, 1...

## 9. How to Stack Toy Boxes (ConcatAxis)

When you have many toy boxes, you can stack them:

-   **`ConcatRows`:** Put one box on top of another.
-   **`ConcatColumns`:** Put boxes next to each other.

## 10. How to Join Toy Boxes (JoinType)

When you have two toy boxes and want to find toys that match, Sara can join them in different ways:

-   **`InnerJoin`:** Only keep toys that are in *both* boxes.
-   **`LeftJoin`:** Keep all toys from the *first* box, and only matching ones from the second.
-   **`RightJoin`:** Keep all toys from the *second* box, and only matching ones from the first.
-   **`OuterJoin`:** Keep *all* toys from *both* boxes, even if they don't have a match.

## 11. How to Sort Your Toys (SortCriterion)

This is a special rule that tells Sara how to sort your toys. You tell it which toy (column) to sort by and if you want it from smallest to biggest (`Ascending`) or biggest to smallest (`Descending`).

## 12. A Toy That Can Be Sorted (SortableColumn)

This is just a fancy way of saying a toy (column) that Sara knows how to sort.

## 13. Knowing All Your Toys (KnownColumns)

This is how Sara knows all the names and types of toys in your big toy box. It helps Sara make sure everything is correct.

## 14. Can This Be a Toy Piece? (CanBeDFValue)

This is a special rule that tells Sara if something can be a `DFValue` (a toy tag). It makes sure only the right kinds of things can be put into your toy box.

## 15. Adding Toy Lists Together (Append)

This is like taking two lists of toys and sticking them together to make one long list.

## 16. Taking Toys Out of a List (Remove)

This is like taking a specific toy out of your list of toys.

## 17. Getting Rid of Duplicate Toys (Nub)

If you have the same toy listed many times, this is like making sure it's only listed once.

## 18. Does This Toy Exist? (HasColumn)

This is how Sara checks if a toy (column) is really in your big toy box. If it's not there, Sara will tell you right away!

## 19. Do All These Toys Exist? (HasColumns)

This is like checking if a whole list of toys are all in your big toy box.

## 20. What Will the New Toy Box Look Like? (JoinCols)

When you join two toy boxes, this helps Sara figure out what the new, combined toy box will look like.

## 21. What Kind of Toy Is This? (TypeOf)

This is how Sara knows exactly what kind of toy (like a car, a block, or a doll) is in a specific column.

## 22. Just the Toy Names (MapSymbols)

This is like getting a list of just the names of your toys, without knowing what kind they are.

## 23. Changing a Toy's Type (UpdateColumn)

If you change a toy from a 'red car' to a 'blue car', this helps Sara update its type in the toy box.

## 24. A Row with Smart Labels (TypeLevelRow)

This is a special kind of row where each toy piece has a super smart label that Sara understands very well.

## 25. Making Smart Labels from Regular Ones (toTypeLevelRow)

This is how Sara takes a regular toy row and gives each toy piece a super smart label.

## 26. Making Regular Labels from Smart Ones (fromTypeLevelRow)

This is how Sara takes a toy row with super smart labels and turns them back into regular ones.

## 27. Is This Toy Missing? (isNA)

This is how Sara checks if a toy piece is missing from your box.

## 28. Just the Toy Types (MapTypes)

This is like getting a list of just the types of your toys (like 'car', 'block', 'doll'), without knowing their names.

## 29. All These Toys Have This Rule (All)

This is a special rule that says "all these toys must follow this rule."

## 30. Is This Toy in the Box? (ContainsColumn)

This is another way Sara checks if a toy (column) is in your big toy box.

## 31. What Kind of Toy Will It Be After Joining? (ResolveJoinValue)

When you join two toy boxes, this helps Sara figure out what kind of toy a piece will become in the new, combined box.

## 32. What Kind of Toy Will the New Column Be After Joining? (ResolveJoinValueType)

Similar to the last one, but this helps Sara figure out the type of a whole new column after joining toy boxes.

