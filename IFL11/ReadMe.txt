HdpH ReadMe


PRE-REQUISITES

* GHC (tested with GHC 6.12.3, 7.2.1 and 7.4.1)

* Haskell Platform compatible with GHC (or at least the following packages
  that would come with the platform: deepseq, mtl, network, parallel,
  parsec, random, stm, transformers)

* MPI installation (tested with MPICH2 1.2.x and OpenMPI 1.4.3)


BUILDING HdpH

There are a number of Makefiles provided, some variables may need adapting:
* HC: GHC command
* CC: C compiler for MPI libs
* CLIBS: MPI libraries to link against
* CLIBPATH: list of paths to MPI libraries; first entry must be subdir 'MP'

Other variables that may be changed:
* HCFLAGS: Add -DHDPH_DEBUG to make HdpH produce low-level debug output.
* CFLAGS:  Add -DMPI_DEBUG to make MPI layer produce low-level debug output.

To build HdpH and all test programs, type 'make all'.


RUNNING HdpH

The directory TEST/HdpH contains a few simple demonstrators. To run these,
set the environment variable LD_LIBRARY_PATH (controling linker behaviour)
to "MP". (Or to "MP:/path/to/MPI/libraries" if the linker does not know
where the MPI libs are.) Eg. in bash, I type

$> export LD_LIBRARY_PATH=MP

Then, in the HdpH root directory, invoke the executable via mpiexec;
depending on your MPI implementation you may need to adapt parameters
and flags in the example below. For instance, to compute the value of
the Fibonacci function at 45 (with a sequential threshold of 30 and a
shared-memory threshold of 35) on 3 nodes, each with 4 cores, I type

$> time mpiexec -n 3 TEST/HdpH/fib -scheds=4 -d1 v2 45 30 35 +RTS -N4

The argument "v2" runs fib in distributed memory mode; the option "-d1"
causes output of some statistics. I get the following output from the
above command:

  {v2, seqThreshold=30, parThreshold=35} fib 45 = 1836311903 {runtime=3.482675s}
  <rank0@137.195.143.109> #SPARK=55   max_SPARK=15   max_THREAD=[1,3,3,3,3]
  <rank0@137.195.143.109> #FISH_sent=9   #SCHED_rcvd=7
  <rank1@137.195.143.125> #SPARK=33   max_SPARK=7   max_THREAD=[1,3,3,3,3]
  <rank1@137.195.143.125> #FISH_sent=17   #SCHED_rcvd=16
  <rank2@137.195.143.131> #SPARK=55   max_SPARK=16   max_THREAD=[2,3,3,3,3]
  <rank2@137.195.143.131> #FISH_sent=6   #SCHED_rcvd=5

  real    0m4.032s
  user    0m0.063s
  sys     0m0.017s

The first line is the result of the Fibonacci function, the following three
pairs of lines report the following statistics for each node:
  #SPARKS:     sparks generated on the node
  max_SPARK:   max size of spark pool
  max_THREAD:  list of max sizes of thread pools (1 for msg handler + 1/core)
  #FISH_sent:  FISH messages sent (looking for work)
  #SCHED_rcvd: SCHEDULE messages received (carrying one spark each)

The time to run the whole application (as measured by the Unix 'time' command)
was 4 seconds and 32 milliseconds; this includes MPI and HpdH startup and
shutdown. The runtime reported on the first line of output, 3.482675 seconds,
excludes MPI startup and shutdown time.


KNOWN PROBLEMS

* HdpH built with GHC 6.12.3 and MPICH2 1.2.x on 64bit Linux machines tends
  to get stuck during the MPI shutdown phase. The cause of the problem is
  yet unknown. GHC 7.2.1 does not appear to have this problem.

* Running HdpH on all cores of a multicore machine (eg. running with
  4 schedulers on a quad-core) often results in extremely variable runtimes.
  The exact cause is yet unknown but it appears related to a known GHC
  phenomenon, the "last core slowdown". For now, the workaround is to
  avoid running on all cores (eg. use only 3 cores of a quad-core).


DIRECTORY STRUCTURE

<HdpH root>	main modules (HdpH and HdpH_IO), Makefiles, ReadMe, etc.

HdpH		exposed modules (Closure, Conf, Strategies)

HdpH/Closure	internal modules implementing explicit Closures

HdpH/Internal	internal modules implementing scheduler, registry, etc.

MP		internal modules providing a message passing abstraction;
		wrapping an MPI library

TEST/MP		test suite for message passing abstraction

TEST/HdpH	simple demonstrators for HdpH (serves as tiny test suite)

TEST/HdpH_IO	simple demonstrators for HdpH_IO

lib		source of some hackage packages (currently only cereal)
		that don't come with the Haskell platform; they are not
		installed by cabal because I could not get cabal to
		work with GHC 7.2.1.


HdpH SYSTEM ARCHITECTURE

HdpH builds on the shared-memory Par monad, the main functionality of
which is synchronisation and scheduling of threads and on-demand work
stealing between cores.  HdpH extends this with remote communication
and synchronisation (via global IVars) and with remote on-demand work
stealing (fishing for sparks).  The result is a Haskell level runtime
system with a two-tier workpool (for threads as well as sparks) and a
registry for global objects (like global IVars).

The work distribution functionality is encapsulated in a monad 'RTS',
defined in 'HdpH.Internal.Scheduler'. 'RTS' really is a stack of 3
monads on top of 'IO':
* 'ThreadM', the thread pool monad (module 'HdpH.Internal.Threadpool'),
* 'SparkM', the spark pool monad (module 'HdpH.Internal.Sparkpool'), and
* 'CommM', encapsulating communication via message passing
  (module 'HdpH.Internal.Comm').
Note that on an n-core node, there can be up to n IO threads running
schedulers (each with their own thread pool). However, there is only
one spark pool, and there is one dedicated IO thread handling incoming
messages.

Communication and synchronisation functionality is provided by the
module 'HdpH.Internal.IVar', implementing both shared-memory IVars and
remote writable global IVars. The latter relies on a registry for
global references (module 'HdpH.Internal.GRef'). Currently, there is
one registry per node.

'HdpH.Internal.Location' exposes locations (aka node IDs) to the system.
This module is at the bottom of the dependency hierarchy because node IDs
crop up almost anywhere (and be it only to be attached to debug messages).

Explicit Closures are the data structure underlying both remote work
distribution and remote communication. They are provided by the
exposed module 'HdpH.Closure', which depends on 'HdpH.Closure.Static'
for a workaround of the as yet missing 'Static' support in GHC.
Explicit Closures are independent of the other HdpH modules. That is,
'HdpH.Closure' and its dependencies (in the directory 'Hdph/Closure')
can be compiled and used separately from HdpH.
