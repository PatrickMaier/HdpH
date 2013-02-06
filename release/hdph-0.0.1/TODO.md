Haskell Distributed Parallel Haskell
====================================

To Do List
----------

* Clean up build.

    * Add `-Wall` switch to cabal file.
    * Suppress spurious warnings about orphan instances.
    * Remove unnecessary monad stack in the implementation of HdpH RTS.

* The work stealing protocol of HdpH is asynchronous and message based.
  TCP isn't a perfect fit; a message based transport protocol (like UDP)
  might be better, at least for small messages (ie. message size < MTU).

* Currenly, the `runParIO` function initiates startup and shutdown of the
  distributed HdpH VM. It might be beneficial to separate VM startup/shutdown
  from any actual parallel computation.
