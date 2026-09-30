# Fuzzing

The extension includes experimental [libFuzzer](https://llvm.org/docs/LibFuzzer.html)
harnesses for the functions parsing t-digests sent by clients, i.e. the text
input function `tdigest_in` and the binary receive function `tdigest_recv`.
Those have to handle arbitrary (possibly malicious) input, which makes them
natural targets for fuzzing.

| Fuzzer              | Harness       | Fuzzed function     | Seed corpus   |
| ------------------- | ------------- | ------------------- | ------------- |
| `fuzz_tdigest_in`   | `fuzz_in.c`   | `tdigest_in_fuzz`   | `corpus/in`   |
| `fuzz_tdigest_recv` | `fuzz_recv.c` | `tdigest_recv_fuzz` | `corpus/recv` |

The `tdigest_in_fuzz` and `tdigest_recv_fuzz` wrappers (at the end of
`tdigest.c`, compiled only for the fuzzers) call `tdigest_in` or
`tdigest_recv`, and if the input is accepted, they add 1000 values to the
digest and compact it. So the fuzzers check not only that the parsing is
safe, but also that the digests it accepts are safe to use.


## How it works

Each fuzzer is a standalone program, not a module loaded into a server. The
harness, an instrumented copy of `tdigest.c` and all the object files of the
PostgreSQL backend are linked into a single executable, with libFuzzer
providing `main()`. The harness sets up just the memory contexts and the
stack depth check, and then libFuzzer calls it for each generated input.
Invalid inputs are expected to fail with an `ERROR`, which the harness
catches and discards, much like the backend would. The fuzzer looks for
inputs causing crashes, assertion failures, errors detected by the
sanitizers, or a `WARNING` (the harness makes those crash, see
[Limitations](#limitations)).

This means that:

* PostgreSQL has to be built from source with clang and the sanitizers, and
  its build directory has to be kept after installation (the fuzzers are
  linked with the backend object files, which are not installed).

* The extension code is compiled with the same compiler and flags as
  PostgreSQL, plus the coverage instrumentation guiding the fuzzer
  (`-fsanitize=fuzzer-no-link`), and with
  `-DFUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION` (the usual macro for fuzzing
  builds), which enables the wrappers. The backend itself is not
  instrumented for coverage.

* No server or database is involved, so there is no need to install the
  extension or run `initdb` to fuzz it.


## Requirements

* Linux (tested on Debian 13, x86_64)

* a git clone of the extension (the release archives created by `make dist`
  do not include the fuzzing harnesses and corpus)

* clang, including the libFuzzer and sanitizer runtimes (compiler-rt)

* `llvm-symbolizer`, for stack traces with function names and line numbers

* the tools needed to build PostgreSQL from a tarball: make, perl, bison and
  flex (tarballs of PostgreSQL 17 and later do not include generated parsers)

On Debian 13 or Ubuntu 24.04 (and later) install:

```sh
sudo apt-get install clang libclang-rt-dev llvm make perl bison flex wget
```

These are metapackages for the distribution's default LLVM version. The
versioned packages can be installed instead (e.g. `clang-19`,
`libclang-rt-19-dev` and `llvm-19`), but then the compiler is `clang-19`, so
use that instead of `clang` below.

To check that clang can build fuzzers with the sanitizers, run:

```sh
echo 'int LLVMFuzzerTestOneInput(const char *d, unsigned long n) { return 0; }' |
    clang -fsanitize=fuzzer,address,undefined -x c - -o /dev/null
```

It should not print anything. An error like `cannot find
.../libclang_rt.fuzzer.a` means the runtimes are missing (`libclang-rt-dev`).


## Building PostgreSQL

Build PostgreSQL with clang, AddressSanitizer (ASan), UndefinedBehaviorSanitizer
(UBSan) and assertions, and install it into a separate directory, e.g.
`$HOME/pg-fuzz`:

```sh
wget https://ftp.postgresql.org/pub/source/v18.6/postgresql-18.6.tar.gz
tar xzf postgresql-18.6.tar.gz
cd postgresql-18.6

export ASAN_OPTIONS=detect_leaks=0

./configure --prefix=$HOME/pg-fuzz --enable-debug --enable-cassert \
    --without-icu --without-readline --without-zlib \
    CC=clang \
    CFLAGS="-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize=function -fno-sanitize-recover=all" \
    LDFLAGS="-fsanitize=address,undefined"

make -j$(nproc)
make install
```

* `ASAN_OPTIONS=detect_leaks=0` disables LeakSanitizer, which reports
  (harmless) leaks in PostgreSQL programs and makes them fail. This matters
  for `pg_config` in particular, which then often loses its output, breaking
  the build of the extension. Keep it set in the following steps too.

* `-fno-sanitize-recover=all` makes UBSan abort on the first error, so that
  libFuzzer treats it as a crash (by default UBSan prints a message and
  continues).

* `-fno-sanitize=function` disables a check included in
  `-fsanitize=undefined` since clang 17, which PostgreSQL does not pass
  (`initdb` fails with `runtime error: call to function string_compare
  through pointer to incorrect function type`).

* `--enable-cassert` enables assertions (both in PostgreSQL and in the
  extension) and memory context checks (see [Limitations](#limitations)).

* `--without-icu --without-readline --without-zlib` only reduce the build
  dependencies. The fuzzers don't need those libraries, but they can be
  enabled if the development packages are installed.

* Use `configure`, not meson. The fuzzers are linked with the backend object
  files listed in the `objfiles.txt` files, and only the make-based build
  generates those.

* Keep the build directory (here `postgresql-18.6`) after `make install`.


## Building the fuzzers

In the extension source directory, run `make fuzz` with the `pg_config` of
the installation built above (or put `$HOME/pg-fuzz/bin` first in `PATH`):

```sh
export ASAN_OPTIONS=detect_leaks=0
make fuzz PG_CONFIG=$HOME/pg-fuzz/bin/pg_config
```

This builds `fuzz_tdigest_in` and `fuzz_tdigest_recv` in the current
directory. The PostgreSQL build directory is determined using the installed
`Makefile.global` (it's the directory where `configure` was run). If it was
moved since then, set `PG_BUILDDIR`:

```sh
make fuzz PG_CONFIG=$HOME/pg-fuzz/bin/pg_config PG_BUILDDIR=/path/to/postgresql-18.6
```

* The fuzzers are built from separate object files (`tdigest_fuzz.o` etc.),
  so `make fuzz` does not affect the regular build of the extension.

* After rebuilding PostgreSQL, `make fuzz` relinks the fuzzers. After
  reconfiguring PostgreSQL (e.g. with different flags), changing its headers,
  or switching to a different PostgreSQL build, run `make clean` first,
  because make does not detect those changes.

* `make clean` removes the fuzzers and their object files, but not the
  fuzzing results (`fuzz-out/`, crash files and logs).


## Running the fuzzers

```sh
export ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=print_stacktrace=1
mkdir -p fuzz-out/in fuzz-out/recv

./fuzz_tdigest_in -artifact_prefix=fuzz-out/ fuzz-out/in corpus/in
./fuzz_tdigest_recv -artifact_prefix=fuzz-out/ fuzz-out/recv corpus/recv
```

libFuzzer loads the inputs from all the directories, and then generates new
inputs by mutating them. New inputs increasing the coverage are written into
the first directory, which is why it should be a scratch directory, not the
seed corpus. The fuzzer runs until it finds a failure or until interrupted
(Ctrl-C). The input causing the failure is saved in a file, with a path
starting with the `-artifact_prefix` (the current directory by default). The
`fuzz-out/` directory and the default names of those files are ignored by
git.

`print_stacktrace=1` adds stack traces to UBSan reports. Leak detection is
not useful for the fuzzers, because all the memory is allocated by `palloc`
and released by resetting the memory context after each input.

Some useful options (`-help=1` lists all of them):

* `-max_total_time=N` - stop after N seconds

* `-jobs=N -workers=N` - run N fuzzer processes in parallel (sharing the
  corpus directory), with the output of each in `fuzz-<job>.log`

* `-max_len=N` - maximum length of the generated inputs (by default the size
  of the largest seed input, but at least 4096 bytes)

* `-timeout=N` - treat inputs taking more than N seconds as failures
  (default 1200)

* `-rss_limit_mb=N` - memory limit (default 2048). The fuzzers use about
  450MB, mostly for the ASan quarantine of freed memory.


## Failures

When the fuzzer finds an input causing a crash, assertion failure, a
sanitizer error or a `WARNING`, it prints the report with a stack trace,
saves the input as `fuzz-out/crash-<sha1>` and stops. Similarly, inputs
exceeding the time or memory limits are saved as `fuzz-out/timeout-<sha1>`
and `fuzz-out/oom-<sha1>`.

To reproduce the failure, run the fuzzer with the file (this runs only the
inputs given as files, without fuzzing):

```sh
./fuzz_tdigest_in fuzz-out/crash-<sha1>
```

To find a smaller input causing the same failure, run:

```sh
./fuzz_tdigest_in -minimize_crash=1 -runs=10000 -artifact_prefix=fuzz-out/ fuzz-out/crash-<sha1>
```

The smaller inputs are saved as `fuzz-out/minimized-from-<sha1>`, and the
last `CRASH_MIN` message names the smallest one found.

The inputs of `fuzz_tdigest_in` are strings in the text format of the
`tdigest` type, e.g. `flags 1 count 2 compression 100 centroids 2 (0.1, 1)
(0.9, 1)`. The inputs of `fuzz_tdigest_recv` are binary, in the format
produced by `tdigest_send` (use e.g. `od -A d -t x1` to inspect them).


## Seed corpus

The seed inputs in `corpus/in` and `corpus/recv` are t-digests in the text
and binary formats. `generate-corpus.sh` generates new ones: it builds 100
digests with random compression and number of values, and writes them into
`corpus/in/<N>` and `corpus/recv/<N>` (relative to the current directory).
It uses `psql` to connect to a database `test` with the extension installed
(the usual libpq environment variables like `PGHOST` and `PGPORT` apply). The
server does not need to be the sanitizer build.

To add only the new inputs that increase the coverage, generate them into a
scratch directory, and merge them into the corpus using the fuzzers:

```sh
createdb test
psql -c 'CREATE EXTENSION tdigest' test

mkdir -p fuzz-out/gen
(cd fuzz-out/gen && ../../generate-corpus.sh)

./fuzz_tdigest_in -merge=1 corpus/in fuzz-out/gen/corpus/in
./fuzz_tdigest_recv -merge=1 corpus/recv fuzz-out/gen/corpus/recv
```

Interesting inputs found by fuzzing can be added the same way, e.g.
`./fuzz_tdigest_recv -merge=1 corpus/recv fuzz-out/recv`. The merge only
adds new files to the first directory, it does not remove any inputs from
the corpus.


## Adding a fuzzer

Add a wrapper to the `FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION` section at
the end of `tdigest.c` (like `tdigest_in_fuzz`), calling the functions to
fuzz. It gets the input as the first argument, either as a C string
(`fuzz_in.c`) or as a `StringInfo` (`fuzz_recv.c`). Then add the fuzzer name
to `FUZZ_TARGETS` in `Makefile`, with a rule compiling the harness for the
wrapper:

```make
fuzz_tdigest_foo.o: fuzz_in.c
	$(CC) $(CFLAGS) $(CPPFLAGS) $(FUZZ_CFLAGS) -DFUZZ_IN_SYMBOL=tdigest_foo_fuzz -c -o $@ $<
```

Finally, add the fuzzer to `.gitignore`, and a seed corpus to `corpus/`.


## Limitations

* The harness does not start a real backend (no catalog access, no
  transactions, no configuration parameters etc.), so it's suitable only for
  functions that don't need those, such as the input functions.

* Only the extension code is instrumented for coverage, so the fuzzer gets no
  feedback from the backend functions it calls (e.g. `float8in`).

* ASan does not detect overflows of chunks allocated by `palloc`, because the
  memory contexts allocate memory from `malloc` in larger blocks. With
  `--enable-cassert`, the memory contexts detect some of those (e.g. writes
  just past the end of a chunk), but only later, when the chunk is freed or
  when the context is reset after each input. That's reported as a `WARNING`
  (e.g. `detected write past chunk end in FuzzContext`), which the harnesses
  treat as a failure. But the stack trace shows where the problem was
  detected, not where the write happened.

* Only Linux, with PostgreSQL built using `configure` (not meson). Tested
  with PostgreSQL 18 and clang 19.


## Troubleshooting

* `unrecognized argument to '-fsanitize=' option: 'fuzzer-no-link'` - The
  PostgreSQL installation (`PG_CONFIG`) was built with gcc. The fuzzers are
  built with the same compiler, so it has to be clang. This may also be
  caused by `pg_config` failing (see the next item).

* `ERROR: LeakSanitizer: detected memory leaks` in `pg_config` or other
  PostgreSQL programs - Set `ASAN_OPTIONS=detect_leaks=0`. The leak check
  makes `pg_config` fail and lose its output, and then the build fails with
  unrelated errors.

* `.../src/timezone/objfiles.txt not found, set PG_BUILDDIR to the PostgreSQL
  build tree` - The PostgreSQL build directory was moved or removed, or the
  installation was not built using `configure` (e.g. it's a distribution
  package, or it was built using meson).

* `runtime error: call to function ... through pointer to incorrect function
  type` - PostgreSQL was built without `-fno-sanitize=function`.

* Stack traces without function names (only `(.../fuzz_tdigest_in+0x...)`) -
  `llvm-symbolizer` was not found. Install it (the `llvm` package), or set
  `ASAN_SYMBOLIZER_PATH` to its location.
