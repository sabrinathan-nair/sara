# ✅ Solution to Parse Error in `Sara/DataFrame/IO.hs`

## 🔍 Problem Summary

The build fails with a GHC parse error at the `of` keyword in a `case` expression, inside the `readCsvStreaming` function.

### ❌ Problematic structure:

```haskell
let dfValues = recordToDFValueList rec
case dfValues of ...
```
This is invalid Haskell. A `case` cannot directly follow a `let` binding without an `in` keyword. Moreover, `let` and `case` are being interleaved incorrectly.

---

## ✅ Corrected Version

Here is the **fixed and properly nested** version of the code:

```haskell
Right $ S.for (S.each (V.toList records)) $ ec -> do
    let dfValues = recordToDFValueList rec
    case dfValues of
        [IntValue eid', TextValue n', TextValue dn', DoubleValue s', DateValue sd', TextValue e'] ->
            case validateEmployee (fromIntegral eid', n', dn', s', sd', e') of
                Left errs -> fail $ unlines $ map show errs
                Right validated -> do
                    let rowMap = Map.fromList
                            [ ("employeeID", IntValue (fromIntegral (veEmployeeID validated))) 
                            , ("name", TextValue (veName validated))
                            , ("departmentName", TextValue (T.pack $ show (veDepartmentName validated)))
                            , ("salary", DoubleValue (fromIntegral (veSalary validated)))
                            , ("startDate", DateValue (veStartDate validated))
                            , ("email", TextValue (T.pack $ show (veEmail validated)))
                            ]
                    let df = DataFrame (Map.map V.singleton rowMap)
                    S.yield df
        _ -> fail "Mismatched EmployeesRecord fields"
```

---

## ✅ Why It Works

- `let` is used only to bind a pure value (`dfValues`).
- `case` is used to match on `dfValues` result **outside** the `let` scope.
- Each block returns an appropriate `IO ()` inside a `Stream`, as expected by `Streaming.Prelude`.

---

## 💡 Tip

If you find yourself deeply nesting logic like this, you might consider refactoring it into a helper function like:

```haskell
processRecord :: EmployeesRecord -> IO (DataFrame cols)
```

This can make the logic more readable and testable.