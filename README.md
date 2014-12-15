HdpH (Haskell distributed parallel Haskell) GitHub repository
=============================================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell but does make use of a few GHC
extensions, most notably TemplateHaskell.

A recent paper [1] presents the semantics of *HdpH* and evaluates its
implementation. There is also an earlier design paper [2]. Sources for
the related reliable *HdpH-RS* DSL can also be found on GitHub [3].


Repository structure
--------------------

This repository follows the branching model of [4]. The `master`
branch contains an archive directory `releases` with released cabal
packages; the `develop` branch holds the current development version;
other branches are not meant to be public.

Package `hdph` is the main HdpH source.  It relies on auxiliary
packages `hdph-closure` and `hdph-mpi-allgather`, where the latter is
only required when using MPI node discovery instead of UDP.

Consult `releases/HISTORY.txt` for information about package and
compiler dependencies.


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


References
----------

1.  Patrick Maier, Rob Stewart, Phil Trinder.
    The HdpH DSLs for Scalable Reliable Computation.
    Proc. 2014 ACM SIGPLAN Symposium on Haskell (Haskell 2014), pp 65-76.
    DOI 10.1145/2633357.2633363

2.  Patrick Maier, Rob Stewart, Phil Trinder.
    [Reliable Scalable Symbolic Computation: The Design of SymGridPar2](http://www.dcs.gla.ac.uk/~pmaier/papers/Maier_Stewart_Trinder_SAC2013.pdf).
    Proc. 28th ACM Symposium On Applied Computing (SAC 2013), pp. 1677-1684.

3.  [HdpH-RS development repository](https://github.com/robstewart57/hdph-rs)

4.  Vincent Driessen.
    [A successful Git branching model](http://nvie.com/posts/a-successful-git-branching-model/)
