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

## 🔹 Type Improvements

- [ ] Introduce `newtype` for:
  - [x] `EmployeeID`
  - [x] `DepartmentName`
  - [x] `Email`
  - [x] `Salary`
- [x] Replace raw primitives in data types with `newtype`s
- [x] Define appropriate `deriving` clauses (`Eq`, `Show`, `Ord`, `Generic`)

---

## 🔹 Smart Constructors

- [x] Add smart constructors for:
  - [x] `Email` (validate format)
  - [x] `Salary` (>= 0)
  - [x] `DepartmentName` (non-empty)
- [x] Replace direct field access with safe construction
- [x] Refactor CSV ingestion pipeline to apply smart constructors

---

## 🔹 Validated Domain Layer

- [x] Define `ValidatedEmployee` data type
- [x] Create `Sara/Validation/Employee.hs` with:
  - [x] `validateEmployee :: Employee -> Either [ValidationError] ValidatedEmployee`
  - [ ] Individual validators for name, email, department
- [ ] Convert parsed records to validated form before further use

---

## 🔹 Error Handling

- [ ] Define `ValidationError` type with constructors:
  - [ ] `InvalidEmail`
  - [ ] `MissingField`
  - [ ] `NegativeSalary`
- [ ] Replace `Either String` with `Validation [ValidationError]`
- [ ] Create `Sara/Error.hs` for shared error types

---

