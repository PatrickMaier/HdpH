The Explicit Closures of Haskell Distributed Parallel Haskell
=============================================================

To Do List
----------

* Clean up build.

    * Add `-Wall` switch to cabal file.

* Improve documentation.

    * Make documentation self-contained; omitting references to package `hdph`.
    * Add self-contained example programs.

* Generate instance declarations for classes like 'ToClosure' via Template
  Haskell. This would avoid the risk of programmers accidentally specifying
  recusive instances.

* Support serialisation via `Data.Binary` in addition (or instead of?)
  `Data.Serialize`.

* Finally: Implement proper compiler support for `Static`.
