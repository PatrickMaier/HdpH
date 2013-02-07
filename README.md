Haskell Distributed Parallel Haskell
====================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell (including GHC extensions).


Repository Structure
--------------------

* Directory `IFL11` contains the code released with the paper [Implementing a High-level Distributed-Memory Parallel Haskell in Haskell](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Trinder_IFL2011_XT.pdf).

* Directory `release` contains cabalized source packages.
  Currently, HdpH is split into two packages:

    * Main package `hdph`.
    * Independent subpackage `hdph-closure`, which factors out the support for explicit closures.


Related Projects
----------------

* [Par Monad and Friends](https://github.com/simonmar/monad-par)

* [Cloud Haskell](http://haskell-distributed.github.com)


References
----------

1.  Patrick Maier, Phil Trinder.
    [Implementing a High-level Distributed-Memory Parallel Haskell in Haskell](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Trinder_IFL2011_XT.pdf).
    Proc. 2011 Symposium on Implementation and Application of Functional Languages (IFL 2011), Springer LNCS 7257, pp. 35-50.

    This paper describes the HdpH design and reports on an initial implementation.

2.  Patrick Maier, Rob Stewart, Phil Trinder.
    [Reliable Scalable Symbolic Computation: The Design of SymGridPar2](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Stewart_Trinder_SAC2013.pdf).
    Proc. 28th ACM Symposium On Applied Computing (SAC 2013), pp. 1677-1684.

    This paper paints the bigger picture of what HdpH was designed for and where future developments will lead.
