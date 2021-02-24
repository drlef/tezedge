# Nom Parsing Exercise

Tests
-----------

To run the tests, in the tezedge directory use
```
cargo test can_deserialize_operations
```
This will run the old tests which use serde and the new tests which use nom. The tests using nom have "\_nom" appended to their names.

Benchmarking
-----------

The performance of the 2 parsers can be benchmarked using
```
cargo bench deserialize
```
From my experience nom runs far faster.

Todo
-----------

This is my first attempt at writing rust, so the code could definitely be more idiomatic.
The while loops in parsing/operations_for_blocks.rs::path() are quite disgusting, but I haven't yet figured out a better way.
Return more instructive error message when parsing errors occur
