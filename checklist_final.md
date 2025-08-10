# ✅ Valid Refactor Checklist for `sara` Project

This checklist contains valid and generic suggestions for improving the `sara` Haskell project.

---

## 🔹 Streaming Implementations

- [ ] Implement streaming for `readJSON`.
- [ ] Implement streaming for `filterRows`.
- [ ] Implement streaming for `filterByBoolColumn`.
- [ ] Implement streaming for `applyColumn`.
- [ ] Implement streaming for `joinDF`.
- [ ] Implement streaming for `sortDataFrame`.
- [ ] Implement streaming for aggregation functions (`sumAgg`, `meanAgg`, `countAgg`).

---



## 🔹 Validated Domain Layer

- [x] Define `ValidatedEmployee` data type
- [x] Create `Sara/Validation/Employee.hs` with:
  - [x] `validateEmployee :: Employee -> Either [ValidationError] ValidatedEmployee`
  - [x] Individual validators for name, email, department
- [x] Convert parsed records to validated form before further use

---

## 🔹 Error Handling

- [x] Define `ValidationError` type with constructors:
  - [x] `InvalidEmail`
  - [x] `MissingField`
  - [x] `NegativeSalary`
- [x] Replace `Either String` with `Validation [ValidationError]`
- [x] Create `Sara/Error.hs` for shared error types

---

