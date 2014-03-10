Haskell Distributed Parallel Haskell
====================================

**Haskell distributed parallel Haskell (HdpH)** is a Haskell DSL for
parallel computation on distributed-memory architectures. HdpH is
implemented entirely in Haskell but does make use of a few GHC extensions,
most notably TemplateHaskell.

HdpH is described in some detail in the paper [Implementing a High-level Distributed-Memory Parallel Haskell in Haskell](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Trinder_IFL2011_XT.pdf).
The paper on [The Design of SymGridPar2](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Stewart_Trinder_SAC2013.pdf) paints the bigger picture of what HdpH was designed for and where future developments will lead.

This release is considered alpha stage.


Building HdpH with UDP node discovery
-------------------------------------

Should be straightforward from the cabalised package `hdph`.
Note that `hdph` depends on `hdph-closure`, an independent package that
factors out the closure representation of HdpH.


Building HdpH with MPI node discovery
-------------------------------------

Requires `hdph-closure` and `hdph-mpi-allgather`. Installation instructions
for the latter depend on the MPI library against which it is linked, see
README of package `hdph-mpi-allgather`.

Once the dependencies have been installed, install `hdph` by passing
`--flags=WithMPI` to the cabal installer.


Running HdpH
------------

### Launch via `ssh`

HdpH comes with a few sample applications. The simplest one,
a distributed _Hello World_, is good for testing whether HdpH works.
For example, the following Bourne shell command will run _Hello World_
on three nodes, distributed over two hosts, `bwlf01` and `bwlf02`.

    $> for host in bwlf01 bwlf02 bwlf01; do ssh $host hdph/dist/build/hello/hello +HdpH -numProcs=3 -HdpH & done

This will launch three instances of the _Hello World_ binary `hello` (in
directory `hdph/dist/build/hello` relative to the user's home directory),
two on `bwlf01` and one on `bwlf02`.
Note that the obligatory option `-numProcs` must match the number of instances;
HdpH will likely hang otherwise.

Assuming that `ssh` has been set up to enable password-less logins, the output
should be something like this:

    Master Node:137.195.143.101:19754:0 wants to know: Who is here?
    Hello from Node:137.195.143.101:19754:0
    Hello from Node:137.195.143.102:38939:0
    Hello from Node:137.195.143.101:30062:0


### Launch via `mpiexec`

Shell scripts and `ssh` are quite a cumbersome way of launching HdpH.
More convenient are dedicated parallel job launchers such as `mpiexec`,
which comes with any recent MPI distribution.
Even though this version of HdpH does not use MPI, it can still be launched
by `mpiexec`.
An MPICH installation, for example, might launch the _Hello World_ application
with the following command:

    $> mpiexec -hosts bwlf01,bwlf02,bwlf01 ./hello +HdpH -numProcs=3 -HdpH

This will launch the _Hello World_ binary `hello` (in the current working
directory) 3 times, twice on `bwlf01` and once on `bwlf02`.
The expected output should look somewhat like this:

    Hello from Node:137.195.143.102:12701:0
    Hello from Node:137.195.143.101:15353:0
    Master Node:137.195.143.101:14247:0 wants to know: Who is here?
    Hello from Node:137.195.143.101:14247:0

#### Note about MPI libraries

If using MPI node discovery please make sure that `mpiexec` actually find
the MPI libraries used to build package `hdph-mpi-allgather`, as mentioned
in the README to `hdph-mpi-allgather`.


### Parallel launch via `mpiexec`

To test parallelism, there is a sample application computing the `n`th number
of the _Fibonacci_ sequence using a naive parallel divide-and-conquer algorithm.
For example, the following command will compute the 45th Fibonacci number in
parallel on 3 nodes, switching to sequential execution for subproblems below
a threshold of 30; a wall clock runtime of about 16 seconds is reported here.

    $> mpiexec -hosts bwlf01,bwlf02,bwlf03 ./fib +HdpH -numProcs=3 -HdpH v2 45 30
    {v2, seqThreshold=30, parThreshold=30} fib 45 = 1836311903 {runtime=16.157814s}

When passed the switch `-d1` HdpH will produce some summary output per
node relating to parallelism, eg. number of sparks generated per node,
maximum number of sparks residing on a node, number of times a node
requested work, and number of times work was scheduled in response.
An example run with `-d1` might look like this:

    $> mpiexec -hosts bwlf01,bwlf02,bwlf03 ./fib +HdpH -numProcs=3 -d1 -HdpH v2 45 30
    137.195.143.101:9759:0 #SPARK=1017   max_SPARK=154   max_THREAD=[3,1]
    137.195.143.101:9759:0 #FISH_sent=1   #SCHED_rcvd=0
    {v2, seqThreshold=30, parThreshold=30} fib 45 = 1836311903 {runtime=16.449183s}
    137.195.143.102:12551:0 #SPARK=308   max_SPARK=4   max_THREAD=[1,1]
    137.195.143.102:12551:0 #FISH_sent=215   #SCHED_rcvd=214
    137.195.143.103:27402:0 #SPARK=271   max_SPARK=4   max_THREAD=[0,1]
    137.195.143.103:27402:0 #FISH_sent=225   #SCHED_rcvd=224

This shows that `bwlf01` (IP address `137.195.143.101`) was the master node,
generating 1017 sparks in total, requesting work once (probably at the end
of the computation) and not receiving any.
The other nodes did generate some sparks themselves (308 and 271, respectively)
but they also both received a substantial number of sparks (214 and 224,
respectively).
Most importantly, each node requested work exactly once more often than
receiving it, which means no node was ever idling, except probably at the
very end of the computation.


### Parallel launch via `mpiexec` on homogeneous multicores

So far HdpH ran in a single thread on each node.
To make use of multicores `mpiexec` can place several single-threaded nodes
on the same host, eg. the following examples will launch two and four nodes per
host, respectively; note that the number following the `-n` switch of `mpiexec`
must match the number following the `-numProcs` switch of `fib`.

    $> mpiexec -hosts bwlf01,bwlf02,bwlf03 -n 6 ./fib +HdpH -numProcs=6 -HdpH v2 45 30
    {v2, seqThreshold=30, parThreshold=30} fib 45 = 1836311903 {runtime=8.026417s}

    $> mpiexec -hosts bwlf01,bwlf02,bwlf03 -n 12 ./fib +HdpH -numProcs=12 -HdpH v2 45 30
    {v2, seqThreshold=30, parThreshold=30} fib 45 = 1836311903 {runtime=4.845796s}

The other possibility is to run HdpH itself in a multi-threaded mode.
The following two examples run HdpH on two and four threads per node,
respectively, expecting that the OS will bind each thread to a core.

    $> mpiexec -hosts bwlf01,bwlf02,bwlf03 ./fib +HdpH -numProcs=3 -scheds=2 -HdpH v2 45 30 +RTS -N2
    {v2, seqThreshold=30, parThreshold=30} fib 45 = 1836311903 {runtime=10.648305s}

    $> mpiexec -hosts bwlf01,bwlf02,bwlf03 ./fib +HdpH -numProcs=3 -scheds=4 -HdpH v2 45 30 +RTS -N4
    {v2, seqThreshold=30, parThreshold=30} fib 45 = 1836311903 {runtime=6.507969s}

Note that the GHC RTS switch `-N` determines the number of threads (or
HECs in GHC terminology) whereas the `fib` switch `-scheds` determines
how many of these threads run the HdpH scheduler loop. Whether there should
be as many schedulers as threads or less depends on the GHC version and
probably on the OS. Using one scheduler less than the number of threads
has been observed to reduce variability, and sometimes even bring
performance gains:

    $> mpiexec -hosts bwlf01,bwlf02,bwlf03 ./fib +HdpH -numProcs=3 -scheds=3 -HdpH v2 45 30 +RTS -N4
    {v2, seqThreshold=30, parThreshold=30} fib 45 = 1836311903 {runtime=5.831932s}


### Parallel launch via `mpiexec` on heterogeneous multicores

So far all hosts were assumed to have the same number of cores.
However, `mpiexec` can be used to launch on heterogeneous clusters.
For instance, the following call will launch HdpH with two threads each on
`bwlf01` and `bwlf02`, and with four threads on `bwlf03`; please see the man
page of `mpiexec` for an explanation of the command line format.

    $> mpiexec -hosts bwlf01,bwlf02,bwlf03 -n 2 ./fib +HdpH -numProcs=3 -scheds=2 -d1 -HdpH v2 45 30 +RTS -N2 : -n 1 ./fib +HdpH -numProcs=3 -scheds=4 -d1 -HdpH v2 45 30 +RTS -N4
    137.195.143.101:21394:0 #SPARK=1114   max_SPARK=183   max_THREAD=[3,1,1]
    137.195.143.101:21394:0 #FISH_sent=3   #SCHED_rcvd=1
    137.195.143.103:22027:0 #SPARK=261   max_SPARK=2   max_THREAD=[0,1,1,1,1]
    137.195.143.103:22027:0 #FISH_sent=209   #SCHED_rcvd=208
    137.195.143.102:22374:0 #SPARK=221   max_SPARK=3   max_THREAD=[1,1,1]
    137.195.143.102:22374:0 #FISH_sent=173   #SCHED_rcvd=172
    {v2, seqThreshold=30, parThreshold=30} fib 45 = 1836311903 {runtime=10.502205s}

The switch `-d1` reveals that `bwlf03` (IP address `137.195.143.103`) did
indeed run 4 schedulers because its `max_THREAD` list was length 5, and
the length of the `max_THREAD` list is always 1 plus the number of schedulers.


### Launch caveats

At startup HdpH nodes open random ports and rely on UDP multicasts to discover
each other. This results in a number of limitations:

+ UDP multicasts must be routed between all nodes (which may imply that all
  hosts must reside in the same subnet).

+ The obligatory `-numProcs` switch, which tells an application how many
  nodes to expect, must match exactly the number of nodes launched.

+ Node discovery must be completed within a certain time frame (typically 10
  seconds).

+ No second HdpH application may be launched during the node discovery phase.

Any deviation from the above limitations is likely to cause HdpH to hang
forever.


Related Projects
----------------

* [Cloud Haskell](http://haskell-distributed.github.com)

* [Par Monad and Friends](https://github.com/simonmar/monad-par)


References
----------

1.  Patrick Maier, Phil Trinder.
    [Implementing a High-level Distributed-Memory Parallel Haskell in Haskell](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Trinder_IFL2011_XT.pdf).
    Proc. 2011 Symposium on Implementation and Application of Functional Languages (IFL 2011), Springer LNCS 7257, pp. 35-50.

2.  Patrick Maier, Rob Stewart, Phil Trinder.
    [Reliable Scalable Symbolic Computation: The Design of SymGridPar2](http://www.macs.hw.ac.uk/~pm175/papers/Maier_Stewart_Trinder_SAC2013.pdf).
    Proc. 28th ACM Symposium On Applied Computing (SAC 2013), pp. 1677-1684.

3.  [HdpH development repository](https://github.com/PatrickMaier/HdpH) on github.
