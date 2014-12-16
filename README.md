HdpH (Haskell distributed parallel Haskell) GitHub repository
=============================================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell but does make use of GHC extensions,
most notably TemplateHaskell.

A recent paper [1] presents the semantics of *HdpH* and evaluates its
implementation. There are also earlier design papers [2,3]. Sources for
the related reliable *HdpH-RS* DSL can also be found on GitHub [4].


Repository structure
--------------------

This repository follows the branching model of [5]. The `master`
branch contains an archive directory `releases` with released cabal
packages; the `develop` branch holds the current development version;
other branches are not meant to be public.

Package `hdph` is the main HdpH source.  It relies on auxiliary
packages `hdph-closure` and `hdph-mpi-allgather`, where the latter is
only required when using MPI node discovery instead of UDP.

Consult `HISTORY.txt` for information about package and compiler
dependencies.


Building HdpH
-------------

Detailed build instructions can be found in `hdph/README.md` and
`hdph/doc/INSTALL.txt`.

Building auxiliary package `hdph-closure` is straightforward but
building `hdph-mpi-allgather` requires furnishing `cabal-install` with
the paths to MPI libraries and includes; see
`hdph-mpi-allgather/README.md` for examples. The package has been
built successfully with recent OpenMPI and MPICH libraries.


Disclaimer
----------

HdpH is not production software; use at your own risk (see `LICENSE`).
Documentation may be out of date.


Related Projects
----------------

* [Cloud Haskell](http://haskell-distributed.github.io)

* [Par Monad and Friends](https://github.com/simonmar/monad-par)


References
----------

1.  Patrick Maier, Rob Stewart, Phil Trinder.
    The HdpH DSLs for Scalable Reliable Computation.
    Proc. 2014 ACM SIGPLAN Symposium on Haskell (Haskell 2014), pp 65-76.
    DOI 10.1145/2633357.2633363

2.  Patrick Maier, Rob Stewart, Phil Trinder.
    [Reliable Scalable Symbolic Computation: The Design of SymGridPar2](http://www.dcs.gla.ac.uk/~pmaier/papers/Maier_Stewart_Trinder_SAC2013.pdf).
    Proc. 28th ACM Symposium On Applied Computing (SAC 2013), pp. 1677-1684.

3.  Patrick Maier, Phil Trinder.
    [Implementing a High-level Distributed-Memory Parallel Haskell in Haskell](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Trinder_IFL2011_XT.pdf).
    Proc. 23rd Intl. Symposium on the Implementation and Application of
    Functional Languages (IFL 2011), pp. 35-50.

4.  Rob Stewart.
    [HdpH-RS development repository](https://github.com/robstewart57/hdph-rs)

5.  Vincent Driessen.
    [A successful Git branching model](http://nvie.com/posts/a-successful-git-branching-model/)
