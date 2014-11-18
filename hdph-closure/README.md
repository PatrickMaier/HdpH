The Explicit Closures of Haskell Distributed Parallel Haskell
=============================================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell but does make use of a few GHC extensions,
most notably TemplateHaskell.

HpdH uses _explicit closures_ to communicate serialised thunks across
the network.
This package exports the fully polymorphic explicit closure representation
of HdpH, for use by HdpH or other packages.

HdpH, including a previous version of the explicit closure representation, is described in some detail in the paper [Implementing a High-level Distributed-Memory Parallel Haskell in Haskell](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Trinder_IFL2011_XT.pdf).

This release is considered alpha stage.


Building HdpH explicit closure support
--------------------------------------

Should be straightforward from the cabalised package `hdph-closure`.


References
----------

1.  Patrick Maier, Phil Trinder.
    [Implementing a High-level Distributed-Memory Parallel Haskell in Haskell](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Trinder_IFL2011_XT.pdf).
    Proc. 2011 Symposium on Implementation and Application of Functional Languages (IFL 2011), Springer LNCS 7257, pp. 35-50.

2.  [HdpH development repository](https://github.com/PatrickMaier/HdpH) on github.
