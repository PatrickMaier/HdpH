HdpH (Haskell distributed parallel Haskell) GitHub repository
=============================================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell but does make use of a few GHC
extensions, most notably TemplateHaskell.

Tag "haskell14" contains cabalized packages for the **HdpH**
DSL, to accompany the recent paper [1]. There exists an earlier
design paper [2]. Sources for the reliable **HdpH-RS** DSL
can also be found on GitHub [3].


Building HdpH
-------------

Detailed build instructions can be found in hdph-0.2.2/README.md and
hdph-0.2.2/INSTALL.txt.

HdpH relies on auxiliary packages hdph-closure and
hdph-mpi-allgather. The former should be straightforward to
install. The latter is only necessary when using MPI node discovery
instead of UDP; it requires a working OpenMPI or MPICH installation.

This release is considered alpha stage. Documentation may be out of
date.


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
