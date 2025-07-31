# Sara Installation Guide (Cabal)

This guide provides detailed instructions on how to set up and install the Sara data manipulation library using Cabal, the standard build tool for Haskell projects.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

1.  **Haskell GHC (Glasgow Haskell Compiler):** The Haskell compiler.
2.  **Cabal:** The Haskell build tool and package manager.

If you don't have GHC and Cabal installed, the recommended way to get them is by using `ghcup`.

### Installing GHC and Cabal with `ghcup`

`ghcup` is a command-line tool that helps you manage different versions of GHC, Cabal, and other Haskell tools. Follow these steps:

1.  **Install `ghcup`:** Open your terminal and run the following command:
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | sh
    ```
    Follow the on-screen prompts. It will ask you to install GHC and Cabal. Accept the defaults unless you have specific reasons not to.

2.  **Restart your terminal or source your shell configuration:** After `ghcup` finishes, you might need to restart your terminal or run `source ~/.bashrc` (or `~/.zshrc`, etc.) to ensure that `ghc` and `cabal` commands are available in your PATH.

3.  **Verify Installation:** Check if GHC and Cabal are installed correctly by running:
    ```bash
    ghc --version
    cabal --version
    ```
    You should see version numbers for both.

## 2. Clone the Sara Repository

First, you need to get the Sara source code. Open your terminal and clone the repository from GitHub:

```bash
git clone https://github.com/sabrinathan-nair/numerology-hs.git
cd numerology-hs
```

## 3. Build and Install Sara

Once you are in the `numerology-hs` directory (which is the root of the Sara project), you can build and install the library using Cabal.

1.  **Update Cabal's package index (optional but recommended):**
    ```bash
    cabal update
    ```

2.  **Build the Sara project:** This command compiles the library and its executables.
    ```bash
    cabal build all
    ```
    This might take some time as Cabal downloads and builds all necessary dependencies.

3.  **Install the Sara project (optional):** If you want to install the executables (like the REPL) to your Cabal binary path (usually `~/.cabal/bin`), you can run:
    ```bash
    cabal install all
    ```
    Note: `cabal build` is usually sufficient for development within the project directory.

## 4. Basic Verification

After building, you can verify that Sara is working by running its test suite or by launching its REPL.

### Running Tests

```bash
cabal test
```

This command will execute the defined test suites for the Sara project. All tests should pass.

### Launching the Sara REPL

If Sara includes a REPL (Read-Eval-Print Loop) executable, you can run it directly:

```bash
cabal run sara-repl # Or whatever the REPL executable is named in sara.cabal
```

This should launch an interactive environment where you can start using Sara functions.

Congratulations! You have successfully set up and installed Sara on your system.
