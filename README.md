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

This repository largely follows the git branching model of [5].

* The `master` branch publishes releases. Its HEAD is always the
  latest release, and it contains a directory `releases` with archived
  cabal packages.

* The `develop` branch tracks development integration. Its HEAD is the
  latest development version.

* Branches named `feature-foo` (where `foo` is a descriptive string)
  are for the development or fixing of feature `foo`. They branch off
  `develop` and are eventually merged back into `develop` (or
  abandoned).

* Branches named `test-bar` (where `bar` is a descriptive string)
  are for testing wacky ideas. They branch off `develop` but are not
  meant to be merged back.

* Branches named `release-a.b.c` (where `a.b.c` is a version number)
  prepare for releases. They branch off `develop` and are merged back
  into `master` and `develop`.

* Branches named `hotfix-a.b.c.d` (`a.b.c.d` is a version number)
  prepare for off-cycle fixes of released code. They branch off `master`
  (version `a.b.c`) and are merged back `master` and `develop`.

* Only branches `master` and `develop` are considered public.


Package `hdph` is the main HdpH source.  It relies on auxiliary
packages `hdph-closure` and `hdph-mpi-allgather`; the latter is
only required when using MPI node discovery instead of UDP.

Consult `HISTORY.txt` for information about package and compiler
dependencies.


Building HdpH
-------------

Detailed build instructions can be found in `hdph/README.md` and
`hdph/doc/INSTALL.txt`.

Building the auxiliary package `hdph-closure` is straightforward.
Building package `hdph-mpi-allgather` requires furnishing the cabal
installer with the paths to MPI libraries and includes, see
`hdph-mpi-allgather/README.md` for examples; the package has been
built successfully with recent OpenMPI and MPICH libraries.

Building With Stack
-------------------

HdpH now supports the building using stack [6]. Building should be as simple as:

```
stack setup #Get an updated GHC if required.
stack build #Build hdph
```

The first time you run stack build it will cache the required
packages. This might take some time.

The executables from HdpH can be found within:

```
./.stack-work/install/x86_64-linux/lts-3.2/7.10.2/bin/
```

or installed into the stack binary folder using:

```
stack install
```

*Note: * Currently you must use the git version of stack (commit later
 than: 603c7916d02) due to a stack bug. This should be fixed in the
 next release.

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
    [The HdpH DSLs for Scalable Reliable Computation](http://www.dcs.gla.ac.uk/~pmaier/#pubs).
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

6.  [Stack Tool](https://github.com/commercialhaskell/stack)
