module Sara.DSL.MutateParser (
    Expr(..),
    parseExpr,
    evaluate
) where

import Sara.DataFrame.Types
import qualified Data.Text as T
import qualified Data.Map as Map
import Data.Char (isSpace, isDigit)

-- | Abstract Syntax Tree for our DSL
data Expr =
    ColRef T.Text          -- Column reference, e.g., "Salary"
    | Lit DFValue          -- Literal value, e.g., 1000
    | BinOp Op Expr Expr   -- Binary operation, e.g., "Salary" / 1000
    deriving (Show, Eq)

-- | Supported binary operations
data Op = Add | Subtract | Multiply | Divide deriving (Show, Eq)

-- | A simple parser for our DSL.
-- This is a basic implementation for demonstration purposes.
-- A more robust solution would use a parser combinator library.
parseExpr :: T.Text -> Either String Expr
parseExpr = parse . T.unpack . T.strip

parse :: String -> Either String Expr
parse str = case parseBinOp str of
    Just (left, op, right) -> do
        leftExpr <- parseExpr (T.pack left)
        rightExpr <- parseExpr (T.pack right)
        return $ BinOp op leftExpr rightExpr
    Nothing -> case parseLit str of
        Just val -> Right (Lit val)
        Nothing -> Right (ColRef (T.pack str))

parseBinOp :: String -> Maybe (String, Op, String)
parseBinOp str = findOp str ['/', '*', '+', '-']
  where
    findOp _ [] = Nothing
    findOp s (opChar:ops) =
        case T.splitOn (T.singleton opChar) (T.pack s) of
            [left, right] -> Just (T.unpack left, charToOp opChar, T.unpack right)
            _ -> findOp s ops

    charToOp '/' = Divide
    charToOp '*' = Multiply
    charToOp '+' = Add
    charToOp '-' = Subtract

parseLit :: String -> Maybe DFValue
parseLit str
    | all isDigit str = Just (IntValue (read str))
    | otherwise = Nothing -- Add more literal types (Double, Bool, etc.) as needed

-- | Evaluates an expression against a given row.
evaluate :: Expr -> Row -> DFValue
evaluate (ColRef colName) row =
    case Map.lookup colName row of
        Just val -> val
        Nothing -> NA -- Or throw an error?
evaluate (Lit val) _ = val
evaluate (BinOp op leftExpr rightExpr) row =
    let leftVal = evaluate leftExpr row
        rightVal = evaluate rightExpr row
    in applyOp op leftVal rightVal

applyOp :: Op -> DFValue -> DFValue -> DFValue
applyOp op (IntValue a) (IntValue b) =
    case op of
        Add -> IntValue (a + b)
        Subtract -> IntValue (a - b)
        Multiply -> IntValue (a * b)
        Divide -> DoubleValue (fromIntegral a / fromIntegral b)
applyOp op (DoubleValue a) (DoubleValue b) =
    case op of
        Add -> DoubleValue (a + b)
        Subtract -> DoubleValue (a - b)
        Multiply -> DoubleValue (a * b)
        Divide -> DoubleValue (a / b)
-- Handle other type combinations as needed
applyOp _ _ _ = NA
