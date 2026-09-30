# Fuzzing t-digest with AFL++

There are two [AFL++](https://aflplus.plus/) harnesses for the functions that
parse untrusted input, both built from `fuzz.c`:

* `fuzz_tdigest_in` - the text input function `tdigest_in`
* `fuzz_tdigest_recv` - the binary receive function `tdigest_recv`, followed
  by `tdigest_send` for accepted values

The harnesses are standalone executables, linking `tdigest.c` with the backend
object files of a PostgreSQL build tree - no server or database is needed.
Rejecting an input with an `ERROR` is the expected outcome. Crashes, failed
assertions and AddressSanitizer errors are reported by the fuzzer, and so is
any `WARNING` or more severe message - the harnesses abort on those, because
the memory context checks in assert-enabled builds report problems like writes
past the end of a chunk as a `WARNING`.

Only Linux is supported, and PostgreSQL has to be built with configure/make
(meson builds don't produce the `objfiles.txt` files the build relies on).


## Packages

On Debian/Ubuntu:

```sh
sudo apt install afl++ build-essential git bison flex perl
```

The `afl++` package also installs `clang`. On other systems, install AFL++ from
the distribution or build it from https://github.com/AFLplusplus/AFLplusplus.


## Build PostgreSQL

The harnesses are linked with the object files of a PostgreSQL build tree, so
PostgreSQL has to be built from source. This was tested with master at commit
`b69356cd789` (2026-09-30).

```sh
TDIGEST=$HOME/tdigest          # this repository
PGSRC=$HOME/fuzz/postgresql    # PostgreSQL source and build tree
PGFUZZ=$HOME/fuzz/pg           # PostgreSQL installation

git clone https://git.postgresql.org/git/postgresql.git $PGSRC
cd $PGSRC
git apply $TDIGEST/postgres-collation.patch    # optional, see below
./configure --prefix=$PGFUZZ --enable-debug --enable-cassert \
    --without-icu --without-readline --without-zlib CC=clang
make -j$(nproc)
make install
```

* Keep the build tree - the harnesses are linked with its object files.
* `postgres-collation.patch` adds `init_database_collation_standalone()`,
  which the harnesses call (if available) to set up the default collation
  without catalogs. tdigest does not use collations, so the patch is not
  required - but the harness is not specific to tdigest, and other data types
  may need it.
* `--enable-cassert` enables assertions and memory context checks, which catch
  bugs AddressSanitizer does not see (see Limitations).
* The `--without-*` options only reduce the number of required packages.
* A gcc build works too, but the harnesses are compiled by clang with the same
  `CFLAGS`, which results in warnings about unknown warning options.
* PostgreSQL itself is not instrumented, only the harness and `tdigest.c` are.
  Instrumenting the whole backend (`CC=afl-clang-fast`) works too, but the
  fuzzing is about half as fast, and `afl-showmap` or `afl-tmin` then require
  `AFL_MAP_SIZE` to be set.


## Build the harnesses

```sh
cd $TDIGEST
AFL_USE_ASAN=1 make fuzz PG_CONFIG=$PGFUZZ/bin/pg_config
```

This builds `fuzz_tdigest_in` and `fuzz_tdigest_recv` with AddressSanitizer.
Make variables:

* `PG_BUILD` - the PostgreSQL build tree, read from the `Makefile.global` of
  the installation. Set this if the build tree was moved.
* `FUZZ_CC` - the compiler, `afl-clang-fast` by default. With `FUZZ_CC=clang`
  the harnesses are not instrumented and run a single input, read from a file
  named on the command line or from stdin (e.g. for debugging).

The harnesses are rebuilt when `tdigest.c`, the harness or the `postgres`
binary change, but not when only `AFL_USE_ASAN` or `FUZZ_CC` change. Run
`make clean PG_CONFIG=$PGFUZZ/bin/pg_config` first in that case.


## Seed inputs

The text format is what `tdigest_out` produces. The binary format (big-endian)
is an int32 flags, int64 count, int32 compression and int32 number of centroids,
followed by float8 mean and int64 count for each centroid.

```sh
mkdir -p $HOME/fuzz/work && cd $HOME/fuzz/work
mkdir seeds-in seeds-recv
printf 'flags 1 count 3 compression 100 centroids 2 (1.5, 1) (2.5, 2)' > seeds-in/small
perl -e 'print "flags 1 count 1000 compression 100 centroids 1000", map { " ($_, 1)" } 1..1000' > seeds-in/large
perl -e 'print pack("N q> N N (d> q>)2", 1, 3, 100, 2, 1.5, 1, 2.5, 2)' > seeds-recv/small
perl -e 'print pack("N q> N N", 1, 1000, 100, 1000), map { pack("d> q>", $_, 1) } 1..1000' > seeds-recv/large
```

Seeds with a trailing newline (or any other trailing bytes) are rejected. The
large seeds are there because digests over 8kB get a separate `malloc()` block,
so AddressSanitizer detects overflows (see Limitations).


## Run the fuzzer

```sh
afl-fuzz -i seeds-in -o out-in -- $TDIGEST/fuzz_tdigest_in
afl-fuzz -i seeds-recv -o out-recv -- $TDIGEST/fuzz_tdigest_recv
```

* The harnesses get the inputs through shared memory (persistent mode), so
  don't use `@@`.
* If `afl-fuzz` refuses to start (e.g. because of `core_pattern`), follow the
  printed instructions, or run `sudo afl-system-config`.
* To use multiple cores, run one instance with `-M main` and others with
  `-S name` (a different name for each), all with the same `-o` directory.
* Stability around 95% is expected and harmless. The first iteration of the
  persistent loop takes a different edge in the harness than the following
  ones, and there are only a few dozen edges in total.


## Reproduce crashes

Inputs that crashed are stored in `out-in/default/crashes/` (similarly for the
other harness and instances). To reproduce, redirect the file to stdin. Don't
use a pipe, because the harness reads the input with a single `read()`:

```sh
$TDIGEST/fuzz_tdigest_in < out-in/default/crashes/id:000000,...
```

To minimize a crashing input:

```sh
afl-tmin -i out-in/default/crashes/id:000000,... -o crash.min -- $TDIGEST/fuzz_tdigest_in
```

AddressSanitizer uses `llvm-symbolizer` to show function names and line
numbers in stack traces. If a report shows only addresses, install the `llvm`
package or set `ASAN_SYMBOLIZER_PATH`.


## Limitations

* `palloc()` allocates small chunks from larger `malloc()` blocks, so
  AddressSanitizer detects only accesses past the end of a whole block.
  Allocations over 8kB have a block of their own.
* In assert-enabled builds, a write to the first byte after a chunk (if the
  chunk has any unused space) is reported by the memory context checks. But a
  write further past the end of a small chunk may go unnoticed.
* Only the input, receive and send functions are fuzzed, not the aggregates or
  other functions working with the digests.
