# BT-Index -- A Burst-Trie Based "Key-Value" Index in RAM

This project is my works for [SIGMOD Programming Contest 2009](http://db.csail.mit.edu/sigmod09contest/).

The goal of the contest is to design an index for main memory data.
The index must be capable of supporting exact match queries and range queries, as well as updates, inserts, and deletes. 
The system must also support serializable execution of user-specified transactions. 

My implementation is based a data structure called "Burst Tire", 
which was introduced by [this paper](http://ww2.cs.mu.oz.au/~jz/fulltext/acmtois02.pdf).

The core API specification can be found in the website of SIGMOD Contest'09,
or you can also see the `server.h` provided by them for details. 

**I have just tested this project on x86-64 arch running Linux, but I think it could be migrated to other POSIX compatible systems easily.**

## TODO

More introduction and more examples ..
