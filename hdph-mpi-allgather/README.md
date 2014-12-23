MPI-based Allgather Operation for Haskell Distributed Parallel Haskell Startup
==============================================================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell but does make use of GHC extensions,
most notably TemplateHaskell.

HpdH uses the *network-transport-tcp* package for node-to-node
communication.  This package relies on a separate node discovery phase
at startup, which is either done by UDP multicast, or (in networks
where multicast isn't feasible) by an MPI Allgather.

This package exports the Allgather used by HdpH via a Haskell binding
to any MPI implementation complying with the MPI2 standard.


Building this package
---------------------

This package has been tested with OpenMPI 1.4.3, MPICH2 1.4.1p1 and
MPICH 3.1.  The cabal file has been deliberately stripped of specifics
of particular MPI implementations, so all information about libraries
and includes must be provided on the command line. (Someone should
write `autoconf` scripts to fix this.)

Here are some sample command lines that I have used successfully to
build hdph-mpi-allgather. See also directory `doc/scripts`.

* On my desktop, with OpenMPI:
  `cabal install --flags=LibOpenMPI --extra-include-dirs=$HOMESW/openmpi/include --extra-lib-dirs=$HOMESW/openmpi/lib`

* On my notebook, with OpenMPI:
  `cabal install --flags=LibOpenMPI --extra-include-dirs=$HOMESW/openmpi/include --extra-lib-dirs=$HOMESW/openmpi/lib`

* At Heriot-Watt, with (a privately installed) MPICH2:
  `cabal install --flags=LibMPICH --extra-include-dirs=$HOMESW/mpich2/1.4.1p1/$ARCH/include --extra-lib-dirs=$HOMESW/mpich2/1.4.1p1/$ARCH/lib`

* At Heriot-Watt, with (a newer, publicly installed) MPICH:
  `cabal install --flags=LibMPICH --extra-include-dirs=/usr/include/mpich-$ARCH --extra-lib-dirs=/usr/lib64/mpich/lib`

* At Glasgow, with (a privately installed) MPICH:
  `cabal install --flags=LibMPICH --extra-include-dirs=$HOMESW/mpich/3.1/$ARCH/include --extra-lib-dirs=$HOMESW/mpich/3.1/$ARCH/lib`


Using this package
------------------

Applications built using this package must be started by the
appropriate MPI application launcher, eg. `mpiexec`. If the MPI
library is linked dynamically, and doesn't reside in a standard
directory, the environment variable `LD_LIBRARY_PATH` may have to be
set accordingly.

Here are some bash command lines that I have used to successfully
start the test application `TestAllgather` on three nodes; build this
application by supplying `--flags=BuildTest` to the cabal commands
above.

* On my desktop, with OpenMPI:
  `LD_LIBRARY_PATH=$HOMESW/openmpi/lib mpiexec -n 3 .cabal-sandbox/bin/TestAllgather`

* On my notebook, with OpenMPI:
  `LD_LIBRARY_PATH=$HOMESW/openmpi/lib mpiexec -n 3 .cabal-sandbox/bin/TestAllgather`

* At Heriot-Watt, with MPICH2:
  `LD_LIBRARY_PATH=$HOMESW/mpich2/1.4.1p1/$ARCH/lib $HOMESW/mpich/1.4.1.p1/$ARHC/bin/mpiexec -n 3 .cabal-sandbox/bin/TestAllgather`

* At Heriot-Watt, with MPICH:
  `LD_LIBRARY_PATH=/usr/lib64/mpich/lib /usr/lib64/mpich/bin/mpiexec -n 3 .cabal-sandbox/bin/TestAllgather`

* At Glasgow, with MPICH:
  `LD_LIBRARY_PATH=$HOMESW/mpich/3.1/$ARCH/lib mpiexec -n 3 .cabal-sandbox/bin/TestAllgather`
