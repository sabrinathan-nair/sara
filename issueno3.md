## Current Problem: Type Mismatch in `Sara/DataFrame/IO.hs`

**Problem Description:**

The project is currently failing to build with type mismatch errors in `Sara/DataFrame/IO.hs`.

**Problematic Code (Excerpt from `Sara/DataFrame/IO.hs`):**

```haskell
-- ... (lines 107-113 omitted for brevity)
                                        [ ("employeeID", IntValue (fromIntegral (veEmployeeID validated)))
                                        , ("name", TextValue (veName validated))
                                        , ("departmentName", TextValue (T.pack $ show (veDepartmentName validated)))
                                        , ("salary", DoubleValue (fromIntegral (veSalary validated)))
                                        , ("startDate", DateValue (veStartDate validated))
                                        , ("email", TextValue (T.pack $ show (veEmail validated)))
                                        ]
-- ... (rest of the file omitted)
```

**Error Message:**

```
src/Sara/DataFrame/IO.hs:108:76: error: [GHC-39999]
    • Could not deduce ‘Integral Sara.Core.Types.EmployeeID’
        arising from a use of ‘fromIntegral’
      from the context: (FromNamedRecord record, HasSchema record,
                         cols ~ Schema record, KnownColumns cols, Generic record,
                         GToFields (Rep record))
        bound by the type signature for:
                   readCsvStreaming :: forall record (proxy :: * -> *)
                                              (cols :: [(ghc-prim-0.13.0:GHC.Types.Symbol, *)]).
                                       (FromNamedRecord record, HasSchema record,
                                        cols ~ Schema record, KnownColumns cols, Generic record,
                                        GToFields (Rep record)) =>
                                       proxy record
                                       -> FilePath
                                       -> IO
                                            (Either
                                               [SaraError] (Stream (Of (DataFrame cols)) IO ()))
        at src/Sara/DataFrame/IO.hs:88:1-261
    • In the first argument of ‘IntValue’, namely
        ‘(fromIntegral (veEmployeeID validated))’
      In the expression: IntValue (fromIntegral (veEmployeeID validated))
      In the expression:
        ("employeeID", IntValue (fromIntegral (veEmployeeID validated)))
    |
108 |                                                 [ ("employeeID", IntValue (fromIntegral (veEmployeeID validated)))
    |                                                                            ^^^^^^^^^^^^

src/Sara/DataFrame/IO.hs:111:75: error: [GHC-39999]
    • Could not deduce ‘Integral Sara.Core.Types.Salary’
        arising from a use of ‘fromIntegral’
      from the context: (FromNamedRecord record, HasSchema record,
                         cols ~ Schema record, KnownColumns cols, Generic record,
                         GToFields (Rep record))
        bound by the type signature for:
                   readCsvStreaming :: forall record (proxy :: * -> *)
                                              (cols :: [(ghc-prim-0.13.0:GHC.Types.Symbol, *)]).
                                       (FromNamedRecord record, HasSchema record,
                                        cols ~ Schema record, KnownColumns cols, Generic record,
                                        GToFields (Rep record)) =>
                                       proxy record
                                       -> FilePath
                                       -> IO
                                            (Either
                                               [SaraError] (Stream (Of (DataFrame cols)) IO ()))
        at src/Sara/DataFrame/IO.hs:88:1-261
    • In the first argument of ‘DoubleValue’, namely
        ‘(fromIntegral (veSalary validated))’
      In the expression: DoubleValue (fromIntegral (veSalary validated))
      In the expression:
        ("salary", DoubleValue (fromIntegral (veSalary validated)))
    |
111 |                                                 , ("salary", DoubleValue (fromIntegral (veSalary validated)))
    |                                                                           ^^^^^^^^^^^^

src/Sara/DataFrame/IO.hs:161:48: error: [GHC-39999]
    • Could not deduce ‘FromJSON EmployeesRecord’
        arising from a use of ‘eitherDecode’
      from the context: KnownColumns cols
        bound by the type signature for:
                   readJSON :: forall (cols :: [(ghc-prim-0.13.0:GHC.Types.Symbol,
                                                 *)]).
                               KnownColumns cols =>
                               Proxy cols -> FilePath -> IO (Either [SaraError] (DataFrame cols))
        at src/Sara/DataFrame/IO.hs:158:1-112
    • In the second argument of ‘first’, namely
        ‘(eitherDecode jsonData)’
      In the expression:
          first (pure . ParsingError . T.pack) (eitherDecode jsonData) ::
            Either [SaraError] [EmployeesRecord]
      In a stmt of a 'do' block:
        case  first
                (pure . ParsingError . T.pack) (eitherDecode jsonData) ::
                Either [SaraError] [EmployeesRecord]
        of
          Left err -> return $ Left err
          Right records
            -> do let ...
                  case validatedRecords of
                    Left errs -> ...
                    Right validated -> ...
    |
161 |     case first (pure . ParsingError . T.pack) (A.eitherDecode jsonData) :: Either [SaraError] [EmployeesRecord] of
    |                                                ^^^^^^^^^^^^^^
```

**Analysis:**

1.  **`fromIntegral` errors**: `veEmployeeID` and `veSalary` are `newtype` wrappers around `Int` and `Double` respectively. `fromIntegral` expects an `Integral` type, but `EmployeeID` and `Salary` are not instances of `Integral`. To fix this, I need to unwrap the `newtype` before using `fromIntegral`.

2.  **`FromJSON EmployeesRecord` error**: This means `EmployeesRecord` does not have a `FromJSON` instance, which is required for `A.eitherDecode` to work. Since `EmployeesRecord` is generated by Template Haskell, I need to ensure it derives `FromJSON`.

**Proposed Solution:**

1.  **Fix `fromIntegral` errors**: In `Sara/DataFrame/IO.hs`, change `fromIntegral (veEmployeeID validated)` to `unEmployeeID (veEmployeeID validated)` and `fromIntegral (veSalary validated)` to `unSalary (veSalary validated)`. Ensure `unEmployeeID` and `unSalary` are imported from `Sara.Core.Types`.

2.  **Add `FromJSON` instance for `EmployeesRecord`**: In `Sara/Schema/Definitions.hs`, ensure that `EmployeesRecord` derives `FromJSON`. This might involve modifying the `inferCsvSchema` call or manually adding the instance if `inferCsvSchema` doesn't support it directly.