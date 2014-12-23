The Explicit Closures of Haskell Distributed Parallel Haskell
=============================================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell but does make use of GHC extensions,
most notably TemplateHaskell.

HpdH uses _explicit closures_ to communicate serialised thunks across
the network.  This package exports the fully polymorphic explicit
closure representation of HdpH, for use by HdpH or other packages.


Building HdpH explicit closure support
--------------------------------------

Should be straightforward from the cabalised package `hdph-closure`.
To build a test suite pass `--flags=BuildTest` to the cabal installer.
