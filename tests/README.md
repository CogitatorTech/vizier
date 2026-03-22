## Tests

This directory contains project tests that are not unit tests like integration, property-based tests, etc.
Unit tests (and regression tests) should be in the same module as the code they test.

### Running Tests

The following commands will run all tests in this directory plus any unit tests in the source modules:

```bash
make test
```

Or

```bash
zig build test
```

### Running Tests by Category

```bash
make test-unit
make test-integration
make test-property
```

Or

```bash
zig build test-unit
zig build test-integration
zig build test-property
```

### Running SQL Tests

SQL tests are in the [sql](sql) directory.

```bash
make test-sql
```
