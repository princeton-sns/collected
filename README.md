# collected

This repository contains implementations for a variety of data processing applications
tuned for a single machine, rather than a distributed system. Problems are chosen to
evaluate how typical distributed workloads can perform on a single machine.

The implements are designed to scale to as many cores and main memory as available.

## Constraints

The implementations try to make few assumptions about the number of available cores,
size of main memory or local caches. When knowing these parameters is useful for
performance, implementations take them as command line arguments.

We assume a NUMA-style architecture: core address shared main memory, but each core
accesses that memory through a heirarchy of caches, starting from a core-exclusive
first level cache to a completely shared last level cache.

When convenient for performance, the implementations assume an x86_64 processor with
64 byte cache lines and 4kB pages.

## Problems:

  - Map Reduce problems
    - [X] Wordcount
    - [ ] Reverse index documents
    - [ ] Log grep
  - _TODO: help think of more_
