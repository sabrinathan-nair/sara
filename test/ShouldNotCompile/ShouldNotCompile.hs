module Main where

import System.Process
import System.FilePath.Posix (takeExtension, dropExtension, (</>))
import System.Directory (listDirectory, doesFileExist)
import Control.Monad (forM_, fail)
import System.Exit (ExitCode(..))
import Data.List (isPrefixOf)
import Data.Maybe (fromMaybe)

main :: IO ()
main = do
    putStrLn "Running should-not-compile tests..."
    testFiles <- listDirectory "."
    let haskellFiles = filter (\f -> takeExtension f == ".hs" && f /= "ShouldNotCompile.hs") testFiles
    
    forM_ haskellFiles $ \file -> do
        putStrLn $ "Testing compilation failure for: " ++ file
        let expectedErrorFile = replaceExtension file ".err"
        expectedErrorExists <- doesFileExist expectedErrorFile
        expectedErrorContent <- if expectedErrorExists
                                then readFile expectedErrorFile
                                else return ""

        let ghcCommand = ["ghc", "-fno-code", "-package", "sara", file]
        (exitCode, stdout, stderr) <- readProcessWithExitCode (head ghcCommand) (tail ghcCommand) ""
        case exitCode of
            ExitSuccess -> do
                putStrLn $ "ERROR: " ++ file ++ " compiled successfully, but was expected to fail!"
                putStrLn stdout
                putStrLn stderr
                fail "Compilation succeeded unexpectedly."
            ExitFailure _ -> do
                putStrLn $ "SUCCESS: " ++ file ++ " failed to compile as expected."
                if expectedErrorExists && not (expectedErrorContent `isPrefixOf` stderr)
                    then do
                        putStrLn $ "ERROR: Expected error message not found or mismatched for " ++ file
                        putStrLn $ "Expected: " ++ expectedErrorContent
                        putStrLn $ "Actual: " ++ stderr
                        fail "Expected error message mismatch."
                    else return ()
    putStrLn "All should-not-compile tests passed."

replaceExtension :: FilePath -> String -> FilePath
replaceExtension fp newExt = dropExtension fp ++ newExt
