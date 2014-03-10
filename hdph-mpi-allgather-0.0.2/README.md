MPI-based Allgather Operation for Haskell Distributed Parallel Haskell Startup
==============================================================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell but does make use of a few GHC extensions,
most notably TemplateHaskell.

HpdH uses the *network-transport-tcp* package for node-to-node communication.
This package relies on a separate node discovery phase at startup, which is
either done by UDP multicast, or (in networks where multicast isn't feasible)
by an MPI Allgather.

This package exports the Allgather used by HdpH via a Haskell binding
to any MPI implementation complying with the MPI2 standard.

This release is considered alpha stage.


Building this package
---------------------

This package has been tested with OpenMPI 1.4.3 and MPICH2 1.4.1p1.
The cabal file has been deliberately stripped of specifics of particular
MPI implementations, so all information about libraries and includes must
be provided on the command line. (Someone should write autoconf scripts to
fix this.)

Here are some sample command lines that I have used successfully to build
hdph-mpi-allgather.

* On my desktop, with OpenMPI:
  `cabal-dev install --flags=LibOpenMPI --extra-include-dirs=$HOME/sw/openmpi/include --extra-lib-dirs=$HOME/sw/openmpi/lib`

* On my notebook, with OpenMPI:
  `cabal-dev install --flags=LibOpenMPI --extra-include-dirs=$HOMESW/openmpi/include --extra-lib-dirs=$HOMESW/openmpi/lib`

* At Heriot-Watt, with MPICH2:
  `cabal-dev install --flags=LibMPICH2 --extra-include-dirs=$HOME/sw/mpich2/1.4.1p1/$ARCH/include --extra-lib-dirs=$HOME/sw/mpich2/1.4.1p1/$ARCH/lib`

* On HECToR, with Cray's own MPICH variant:

    module swap PrgEnv-cray PrgEnv-gnu
    export PATH=~/.cabal/bin:~/sw/ghc/7.6.3/bin:$PATH
    cabal-dev install --flags=LibMPICHCray --extra-include-dirs=$MPICH_DIR/include --extra-lib-dirs=$MPICH_DIR/lib


Using this package
------------------

Applications built using this package must be started by the appropriate
MPI application launcher, eg. `mpiexec`. If the MPI library is linked
dynamically, and doesn't reside in a standard directory, the environment
variable `LD_LIBRARY_PATH` may have to be set accordingly.

Here are some bash command lines that I have used to successfully start
the test application `TestMPI` on three nodes.

* On my desktop, with OpenMPI:
  `LD_LIBRARY_PATH=$HOME/sw/openmpi/lib mpiexec -n 3 ./TestMPI`

* On my notebook, with OpenMPI:
  `LD_LIBRARY_PATH=$HOMESW/openmpi/lib mpiexec -n 3 ./TestMPI`

* At Heriot-Watt, with MPICH2:
  `LD_LIBRARY_PATH=$HOME/sw/mpich2/1.4.1p1/$ARCH/lib mpiexec -n 3 ./TestMPI`

* On HECToR, with Cray's own MPICH variant:
  `aprun -n 3 ./TestMPI`


References
----------

1.  Patrick Maier, Phil Trinder.
    [Implementing a High-level Distributed-Memory Parallel Haskell in Haskell](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Trinder_IFL2011_XT.pdf).
    Proc. 2011 Symposium on Implementation and Application of Functional Languages (IFL 2011), Springer LNCS 7257, pp. 35-50.

2.  [HdpH development repository](https://github.com/PatrickMaier/HdpH) on github.

3.  [network-transport-tcp](https://hackage.haskell.org/package/network-transport-tcp) on Hackage.
