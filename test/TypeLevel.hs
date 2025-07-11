{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module TypeLevel where

import GHC.TypeLits
import Data.Proxy

type family MyFamily (a :: Symbol) :: Symbol
type instance MyFamily "foo" = "bar"

type family MyList (a :: Symbol) :: [(Symbol, Symbol)]
type instance MyList "foo" = '[ '("foo", MyFamily "foo")]

class MyClass (a :: [(Symbol, Symbol)]) where
  myMethod :: Proxy a -> String

instance MyClass '[ '("foo", "bar")] where
  myMethod _ = "bar"

myFunc :: forall (a :: Symbol) (b :: [(Symbol, Symbol)]).
          ( MyClass b
          , b ~ MyList a
          ) => Proxy a -> String
myFunc _ = myMethod (Proxy @b)

main :: IO ()
main = print $ myFunc (Proxy @"foo")
