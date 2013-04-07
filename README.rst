============
Rewind in Go
============

This is the next version of `Rewind`_. The intention is to bring higher
concurrency and support for multiple streams. Initially only a single
storage backend will be supported (LevelDB).

.. _Rewind: https://github.com/JensRantil/rewind

The implementation is written in the Go programming language.

Currently, this project is highly experimental and alpha.

I'll try to stick to http://www.semver.org when it comes to versioning.

Other things:

 * Tests are exeucting using `go test -v ./server`.
